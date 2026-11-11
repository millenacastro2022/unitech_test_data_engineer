"""Camada Gold: o PySpark publica a Silver no PostgreSQL e o dbt modela o Star Schema.

1. ``publicar_silver`` (PySpark): lê da Silver (RustFS) as partições que a Gold ainda não
   carregou e as publica no schema ``silver`` do PostgreSQL via JDBC, substituindo só o
   período que mudou. Soma também os totais da Silver que a validação usa.
2. ``executar_dbt``: ``dbt build`` lê o schema ``silver`` e cria/atualiza dimensões e fatos
   (modelos incrementais em ``dbt/models``), e roda os testes do dbt.
3. ``validar_cargas``: reconcilia a Gold com os totais da Silver e só então registra a carga
   no controle; o que não passa continua pendente para a próxima execução.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path

import psycopg2

from ans_pipeline import storage
from ans_pipeline.config import SQL_DIR, Settings, get_settings
from ans_pipeline.periodo import Competencia

log = logging.getLogger(__name__)

# Projeto dbt (montado em /opt/airflow/dbt no container).
DBT_DIR = Path(os.getenv("ANS_DBT_DIR", Path(__file__).resolve().parents[2] / "dbt"))
DBT_TARGET = Path(os.getenv("DBT_TARGET_PATH", "/tmp/dbt/target"))

# A Gold recebe o plano de contas até o 4º nível (ex.: 3, 31, 311, 3111): suficiente para
# indicadores como sinistralidade, com ~1/6 das linhas. O detalhe completo fica na Silver.
NIVEL_MAX_CONTA_GOLD = 4

COLUNAS_OPERADORAS = ("cd_operadora", "nr_cnpj", "nm_razao_social", "nm_fantasia", "ds_modalidade", "sg_uf", "nm_cidade",
                      "cd_regiao_comercializacao", "dt_registro_ans")
COLUNAS_PRODUTOS = ("cd_operadora", "cd_plano", "id_plano", "nm_plano", "tp_vigencia_plano", "ds_contratacao",
                    "ds_grupo_contratacao", "ds_segmentacao_assistencial", "ds_grupo_segmentacao_assistencial", "ds_cobertura",
                    "fl_odontologico", "ds_obstetricia", "ds_abrangencia_cobertura", "tp_financiamento",
                    "ds_fator_moderador", "ds_acomodacao_hospitalar", "ds_livre_escolha", "ds_porte_operadora",
                    "ds_situacao_plano", "dt_situacao", "dt_registro_plano")


# ---------------------------------------------------------------------- acesso ao DW
def executar_sql_arquivo(relativo: str, parametros: dict | None = None, settings: Settings | None = None) -> None:
    """Executa um arquivo de ``sql/`` no DW em uma transação (commit no sucesso, rollback no erro)."""
    s = settings or get_settings()
    sql = (SQL_DIR / relativo).read_text(encoding="utf-8")
    conn = psycopg2.connect(s.dw_dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, parametros or None)
    finally:
        conn.close()


def executar(sql: str, parametros: tuple | dict | None = None, settings: Settings | None = None) -> None:
    s = settings or get_settings()
    conn = psycopg2.connect(s.dw_dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, parametros)
    finally:
        conn.close()


def consultar(sql: str, parametros: dict | tuple | None = None, settings: Settings | None = None) -> list[tuple]:
    s = settings or get_settings()
    conn = psycopg2.connect(s.dw_dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, parametros)
            return cur.fetchall()
    finally:
        conn.close()


# ---------------------------------------------------------------------- 1) Silver publicada no DW (PySpark)
def publicar_silver(reprocessar: bool = False, settings: Settings | None = None) -> dict:
    """Silver (RustFS) -> schema ``silver`` do PostgreSQL, para tudo o que a Gold ainda não carregou.

    Cada unidade pendente substitui a sua parte no schema: o snapshot inteiro de operadoras e
    produtos, uma competência de beneficiários, um trimestre do financeiro.
    """
    from pyspark.sql import functions as F

    from ans_pipeline import controle
    from ans_pipeline.spark import gravar_jdbc, sessao

    s = settings or get_settings()
    controle.aplicar_ddl(s)
    executar_sql_arquivo("silver/01_silver.sql", settings=s)
    pend = controle.pendentes_gold(reprocessar, s)
    resumo: dict = {"dimensoes": {}, "competencias": [], "trimestres": []}
    if not (pend["dm_operadora"] or pend["dm_produto"] or pend["competencias"] or pend["trimestres"]):
        log.info("Nada pendente na Silver: nada a publicar")
        return resumo

    with tempfile.TemporaryDirectory(prefix="gold_") as tmp, sessao("publicar_silver", s, temp_dir=Path(tmp) / "spark") as spark:
        local = Path(tmp)

        def ler(prefixo: str, pasta: str):
            chaves = [k for k in storage.listar_chaves(s.silver_bucket, prefixo, s) if k.endswith(".parquet")]
            if not chaves:
                raise FileNotFoundError(f"Nenhum dado na Silver em s3://{s.silver_bucket}/{prefixo}")
            arquivos = storage.baixar_objetos(s.silver_bucket, chaves, local / pasta, s)
            return spark.read.parquet(*(str(a) for a in arquivos))

        # cadastros: o snapshot mais recente substitui o anterior
        for unidade, tabela, colunas in (("dm_operadora", "silver.tb_operadoras", COLUNAS_OPERADORAS),
                                         ("dm_produto", "silver.tb_produtos", COLUNAS_PRODUTOS)):
            p = pend[unidade]
            if not p:
                continue
            df = ler(p["uri"].split(f"s3://{s.silver_bucket}/", 1)[1], unidade).select(*colunas).cache()
            executar(f"TRUNCATE {tabela}", settings=s)
            gravar_jdbc(df, tabela, s)
            resumo["dimensoes"][unidade] = {**p, "linhas_publicadas": df.count()}

        # beneficiários: somados por competência x operadora x plano x UF x cobertura x contratação
        for p in pend["competencias"]:
            c = Competencia.parse(p["competencia"])
            df = ler(f"tb_beneficiarios/competencia={c.yyyymm}/", f"benef_{c.yyyymm}")
            total, ufs = df.agg(F.coalesce(F.sum("qt_beneficiario_ativo"), F.lit(0)), F.countDistinct("sg_uf")).first()
            somado = (df.groupBy("dt_competencia", "cd_operadora", "cd_plano", "sg_uf", "ds_cobertura_assistencial", "ds_contratacao")
                      .agg(F.any_value("nm_razao_social").alias("nm_razao_social"),
                           F.any_value("nr_cnpj").alias("nr_cnpj"),
                           F.any_value("ds_modalidade").alias("ds_modalidade"),
                           F.any_value("tp_vigencia_plano").alias("tp_vigencia_plano"),
                           F.any_value("ds_segmentacao").alias("ds_segmentacao"),
                           F.any_value("ds_abrangencia_geografica").alias("ds_abrangencia_geografica"),
                           F.sum("qt_beneficiario_ativo").cast("bigint").alias("qt_beneficiario_ativo"),
                           F.sum("qt_beneficiario_aderido").cast("bigint").alias("qt_beneficiario_aderido"),
                           F.sum("qt_beneficiario_cancelado").cast("bigint").alias("qt_beneficiario_cancelado"),
                           F.max("dt_carga").alias("dt_carga"))
                      .cache())
            executar("DELETE FROM silver.tb_beneficiarios WHERE dt_competencia = %s", (c.primeiro_dia,), s)
            gravar_jdbc(somado, "silver.tb_beneficiarios", s)
            resumo["competencias"].append({**p, "linhas_publicadas": somado.count(),
                                           "silver_ativos": int(total), "silver_ufs": int(ufs)})

        # financeiro: plano de contas até o 4º nível
        for p in pend["trimestres"]:
            ano, tri = p["ano"], p["trimestre"]
            inicio = Competencia(ano, (tri - 1) * 3 + 1).primeiro_dia
            df = ler(f"tb_financeiro/ano={ano}/trimestre={tri}/", f"fin_{ano}_{tri}")
            silver_contas = {r[0]: float(r[1]) for r in df.filter(F.col("cd_conta_contabil").isin("31", "41"))
                             .groupBy("cd_conta_contabil").agg(F.sum("vl_saldo_final")).collect()}
            nivel = (df.filter(F.length("cd_conta_contabil") <= NIVEL_MAX_CONTA_GOLD)
                     .select("dt_trimestre", "cd_operadora", "cd_conta_contabil", "ds_conta_contabil",
                             "vl_saldo_inicial", "vl_saldo_final", "vl_movimento_trimestre")
                     .cache())
            executar("DELETE FROM silver.tb_financeiro WHERE dt_trimestre = %s", (inicio,), s)
            gravar_jdbc(nivel, "silver.tb_financeiro", s)
            resumo["trimestres"].append({**p, "inicio": inicio.isoformat(), "linhas_publicadas": nivel.count(),
                                         "silver_contas": silver_contas})

    log.info("Silver publicada: dimensões %s, competências %s, trimestres %s", list(resumo["dimensoes"]),
             [c["competencia"] for c in resumo["competencias"]], [f"{t['ano']}-T{t['trimestre']}" for t in resumo["trimestres"]])
    return resumo


# ---------------------------------------------------------------------- 2) dbt
class FalhaDbt(Exception):
    """O dbt terminou com erro de modelo ou teste reprovado (código de saída 1)."""


def variaveis_dbt(publicado: dict) -> dict:
    """Períodos que as fatos devem recarregar nesta execução (só o que acabou de ser publicado)."""
    return {
        "competencias": [Competencia.parse(c["competencia"]).primeiro_dia.isoformat() for c in publicado.get("competencias", [])],
        "trimestres": [t["inicio"] for t in publicado.get("trimestres", [])],
    }


def executar_dbt(publicado: dict | None = None, argumentos: tuple[str, ...] = ("build",),
                 settings: Settings | None = None) -> dict:
    """Roda o dbt (virtualenv próprio, veja o Dockerfile) e devolve o resumo do run_results.

    Com ``publicado``, as fatos recarregam só as competências e trimestres publicados agora.
    Código 1 = modelo com erro ou teste reprovado (``FalhaDbt``: repetir não resolve);
    outro código diferente de zero = falha de execução (conexão etc.), que pode ser repetida.
    """
    dbt = os.getenv("DBT_BIN", "dbt")
    env = {**os.environ, "DBT_TARGET_PATH": str(DBT_TARGET), "DBT_LOG_PATH": str(DBT_TARGET.parent / "logs")}
    comando = [dbt, *argumentos, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--no-use-colors"]
    if publicado is not None:
        comando += ["--vars", json.dumps(variaveis_dbt(publicado))]
    log.info("Executando: %s", " ".join(comando))
    proc = subprocess.Popen(comando, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env, cwd=DBT_DIR)
    assert proc.stdout is not None
    for linha in proc.stdout:
        log.info("dbt | %s", linha.rstrip())
    codigo = proc.wait()

    resumo: dict = {}
    resultados = DBT_TARGET / "run_results.json"
    if resultados.exists():
        for r in json.loads(resultados.read_text(encoding="utf-8")).get("results", []):
            tipo = r["unique_id"].split(".")[0]
            resumo.setdefault(tipo, {}).setdefault(r["status"], 0)
            resumo[tipo][r["status"]] += 1
    if codigo == 1:
        raise FalhaDbt(f"dbt build com erros ou testes reprovados: {resumo}")
    if codigo != 0:
        raise RuntimeError(f"dbt terminou com código {codigo}")
    log.info("dbt build ok: %s", resumo)
    return resumo


# ---------------------------------------------------------------------- 3) validação e registro
def validar_cargas(publicado: dict, dag_run_id: str | None = None, settings: Settings | None = None) -> dict:
    """Confere cada unidade carregada pelo dbt contra a Silver e registra no controle."""
    from ans_pipeline import controle, quality

    s = settings or get_settings()
    feitas: dict = {"dimensoes": [], "competencias": [], "trimestres": []}
    for unidade, info in publicado.get("dimensoes", {}).items():
        tabela, chave = ("gold.dm_operadora", "sk_operadora") if unidade == "dm_operadora" else ("gold.dm_produto", "sk_produto")
        total = consultar(f"SELECT count(*) FROM {tabela} WHERE {chave} > 0", settings=s)[0][0]
        controle.registrar_carga_gold(unidade, info["ate"], {"particao": info["particao"],
                                      "publicadas": info["linhas_publicadas"], f"{unidade}_total": total}, dag_run_id, s)
        feitas["dimensoes"].append(unidade)
    for c in publicado.get("competencias", []):
        competencia = Competencia.parse(c["competencia"])
        validacao = quality.validar_beneficiarios(competencia, c["silver_ativos"], c["silver_ufs"], s)
        controle.registrar_carga_gold(f"ft_beneficiarios:{competencia.iso}", c["ate"],
                                      {"publicadas": c["linhas_publicadas"], "validacao": validacao}, dag_run_id, s)
        feitas["competencias"].append(competencia.iso)
    for t in publicado.get("trimestres", []):
        validacao = quality.validar_financeiro(t["ano"], t["trimestre"], t["silver_contas"], s)
        controle.registrar_carga_gold(f"ft_financeiro:{t['ano']}-T{t['trimestre']}", t["ate"],
                                      {"publicadas": t["linhas_publicadas"], "validacao": validacao}, dag_run_id, s)
        feitas["trimestres"].append(f"{t['ano']}-T{t['trimestre']}")
    log.info("Cargas validadas e registradas: %s", feitas)
    return feitas
