"""Camada Bronze: o arquivo da Landing como tabela Parquet, sem limpeza nenhuma (PySpark).

Todas as colunas ficam como texto, como vieram da ANS; só os nomes passam para snake_case
(``Razão Social`` -> ``razao_social``), para o Parquet e o Spark aceitarem. Entram três
colunas de controle: ``dh_ingestao`` (quando o arquivo chegou à Landing), ``ds_arquivo_origem``
(objeto da Landing) e ``dh_processamento`` (quando a Bronze gravou a partição), em UTC. A única verificação é de layout: faltou coluna obrigatória, falha.

Layout:  s3://bronze/<entidade>/<partição>/<entidade>.parquet
Ex.:     s3://bronze/beneficiarios/competencia=202609/uf=SP/beneficiarios.parquet
"""

from __future__ import annotations

import codecs
import logging
import re
import shutil
import tempfile
import unicodedata
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from ans_pipeline import storage
from ans_pipeline.config import Settings, get_settings
from ans_pipeline.entidades import ENTIDADES, Entidade, SchemaDivergente, particao_tabela

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------- leitura do arquivo original
def normalizar_nome_coluna(nome: str) -> str:
    """'VL_SALDO_INICIAL' -> 'vl_saldo_inicial'; 'Razão Social' -> 'razao_social'."""
    sem_acento = unicodedata.normalize("NFKD", nome).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", sem_acento.strip().lower()).strip("_")


def detectar_encoding(caminho: Path, amostra_bytes: int = 8 * 1024 * 1024) -> str:
    """A ANS publica arquivos em UTF-8 (recentes) e Latin-1 (antigos)."""
    with open(caminho, "rb") as f:
        amostra = f.read(amostra_bytes)
        arquivo_inteiro = not f.read(1)
    try:
        # Decodificador incremental: um caractere multibyte cortado no fim da amostra
        # não é tratado como erro (final=False), mas um byte Latin-1 no meio é.
        codecs.getincrementaldecoder("utf-8")().decode(amostra, final=arquivo_inteiro)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def extrair_csv(arquivo: Path, destino: Path) -> Path:
    """Se ``arquivo`` for ZIP, extrai o maior CSV contido nele (sem confiar nos caminhos
    internos do ZIP); senão retorna o próprio arquivo."""
    if not zipfile.is_zipfile(arquivo):
        return arquivo
    with zipfile.ZipFile(arquivo) as z:
        csvs = [i for i in z.infolist() if i.filename.lower().endswith(".csv") and not i.is_dir()]
        if not csvs:
            raise SchemaDivergente(f"Nenhum CSV dentro de {arquivo.name}: {z.namelist()}")
        info = max(csvs, key=lambda i: i.file_size)
        saida = destino / Path(info.filename).name
        with z.open(info) as origem, open(saida, "wb") as alvo:
            shutil.copyfileobj(origem, alvo, length=1024 * 1024)
    return saida


def ler_csv(spark: "SparkSession", csv: Path, entidade: Entidade) -> "DataFrame":
    """O CSV da ANS como veio (todas as colunas como texto), com nomes em snake_case.
    Falha com ``SchemaDivergente`` se faltar alguma coluna obrigatória."""
    encoding = detectar_encoding(csv)
    bruto = (spark.read
             .option("sep", ";").option("header", True).option("quote", '"').option("escape", '"')
             .option("encoding", "UTF-8" if encoding == "utf-8" else "ISO-8859-1")
             .option("mode", "PERMISSIVE")
             .csv(str(csv)))
    normalizadas = [normalizar_nome_coluna(c) for c in bruto.columns]
    faltantes = sorted(set(entidade.colunas_obrigatorias) - set(normalizadas))
    if faltantes:
        raise SchemaDivergente(
            f"{entidade.nome}: colunas obrigatórias ausentes {faltantes}. Colunas recebidas: {normalizadas}"
        )
    log.info("%s: %d colunas, encoding %s", entidade.nome, len(normalizadas), encoding)
    return bruto.toDF(*normalizadas)


# ---------------------------------------------------------------------- uma partição
def processar(spark: "SparkSession", item: dict, dag_run_id: str | None = None,
              settings: Settings | None = None) -> dict:
    """Landing (arquivo original) -> Bronze (Parquet) para um arquivo, e registro no controle.
    A escrita sobrescreve a partição, então reprocessar é idempotente."""
    from pyspark.sql import functions as F

    from ans_pipeline import controle
    from ans_pipeline.spark import gravar_parquet_unico

    s = settings or get_settings()
    entidade = ENTIDADES[item["fonte"]]
    particao = particao_tabela(item["fonte"], item["particao"], item["ingestion_date"])
    chave = f"{entidade.nome}/{particao}/{entidade.nome}.parquet"

    with tempfile.TemporaryDirectory(prefix=f"bronze_{entidade.nome}_") as tmp:
        tmp_path = Path(tmp)
        bruto = storage.baixar_arquivo(s.landing_bucket, item["chave"], tmp_path / "landing" / Path(item["chave"]).name, s)
        csv = extrair_csv(bruto, tmp_path)
        if csv != bruto:
            bruto.unlink()  # libera disco: o ZIP já foi extraído
        tabela = ler_csv(spark, csv, entidade).select(
            "*",
            # controle da Bronze (UTC): quando o arquivo chegou à Landing e quando a Bronze o gravou
            F.lit(item.get("ingerido_em") or item["ingestion_date"]).cast("timestamp_ntz").alias("dh_ingestao"),
            F.lit(f"s3://{s.landing_bucket}/{item['chave']}").alias("ds_arquivo_origem"),
            F.current_timestamp().cast("timestamp_ntz").alias("dh_processamento"),
        )
        arquivo = gravar_parquet_unico(tabela, tmp_path / "saida")
        linhas = spark.read.parquet(str(arquivo)).count()
        if linhas == 0:
            raise SchemaDivergente(f"{entidade.nome} {particao}: arquivo sem linhas ({item['chave']})")
        storage.enviar_arquivo(arquivo, s.bronze_bucket, chave, {"source-key": item["chave"]}, s)

    uri = f"s3://{s.bronze_bucket}/{chave}"
    controle.registrar_bronze(entidade=entidade.nome, particao=particao, uri=uri, chave_landing=item["chave"],
                              linhas=linhas, colunas=len(tabela.columns), dag_run_id=dag_run_id, settings=s)
    log.info("Bronze: %s (%d linhas)", uri, linhas)
    return {"particao": f"{entidade.nome} {particao}", "uri": uri, "linhas": linhas}


# ---------------------------------------------------------------------- o que a DAG chama
def listar_lotes(reprocessar: bool = False, settings: Settings | None = None,
                 fontes: list[str] | None = None) -> list[list[dict]]:
    """Arquivos da Landing que a Bronze ainda não converteu, em lotes por fonte e período.
    ``fontes`` restringe a consulta (padrão: todas)."""
    from ans_pipeline import controle
    from ans_pipeline.fontes import FONTES
    from ans_pipeline.periodo import agrupar_por_periodo

    s = settings or get_settings()
    controle.aplicar_ddl(s)
    storage.garantir_buckets(s)
    lotes = agrupar_por_periodo(controle.pendentes_bronze(list(fontes or FONTES), reprocessar, s))
    log.info("%d lotes: %s", len(lotes), [f"{l[0]['fonte']} {l[0]['particao'].split('/uf=')[0]} ({len(l)})" for l in lotes])
    return lotes


def processar_lote(lote: list[dict], reprocessar: bool = False, dag_run_id: str | None = None,
                   settings: Settings | None = None) -> dict:
    """Um lote numa única sessão Spark. Numa retentativa, pula o que já foi convertido."""
    from ans_pipeline import controle
    from ans_pipeline.spark import sessao

    s = settings or get_settings()
    if not reprocessar:
        ainda = {p["chave"] for p in controle.pendentes_bronze(sorted({i["fonte"] for i in lote}), settings=s)}
        lote = [i for i in lote if i["chave"] in ainda]
    feitos = []
    if lote:
        with tempfile.TemporaryDirectory(prefix="spark_") as tmp, sessao("bronze", s, temp_dir=Path(tmp)) as spark:
            for item in lote:
                feitos.append(processar(spark, item, dag_run_id, s))
    return {"particoes": len(feitos), "linhas": sum(f["linhas"] for f in feitos), "detalhe": feitos}
