"""Tabelas de controle que ligam as quatro DAGs (schema ``controle`` do PostgreSQL).

Cada camada registra o que produziu; a camada seguinte consulta o que ainda não consumiu.
Assim as DAGs rodam em horários diferentes, sem XCom entre elas, e reprocessar é seguro:
um arquivo mais novo de uma partição substitui o anterior em todas as camadas seguintes.

As colunas seguem o padrão de prefixos de 2 letras (``ds_chave``, ``nm_fonte``, ``dh_processamento``...);
as consultas devolvem os registros com as chaves curtas que o resto do pacote usa (``chave``,
``fonte``, ``particao``...).
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime

import psycopg2
import psycopg2.extras

from ans_pipeline.config import SQL_DIR, Settings, get_settings

log = logging.getLogger(__name__)

DDL = "controle/01_controle.sql"
DIMENSOES = {"operadoras": "dm_operadora", "produtos": "dm_produto"}


# ---------------------------------------------------------------------- acesso
def _executar(sql: str, parametros: dict | None = None, settings: Settings | None = None) -> None:
    conn = psycopg2.connect((settings or get_settings()).dw_dsn)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(sql, parametros)
    finally:
        conn.close()


def _consultar(sql: str, parametros: dict | None = None, settings: Settings | None = None) -> list[dict]:
    conn = psycopg2.connect((settings or get_settings()).dw_dsn)
    try:
        with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, parametros)
            return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def aplicar_ddl(settings: Settings | None = None) -> None:
    _executar((SQL_DIR / DDL).read_text(encoding="utf-8"), settings=settings)


# ---------------------------------------------------------------------- Landing
def ultimos_landing(settings: Settings | None = None) -> dict[tuple[str, str], dict]:
    """O arquivo mais recente de cada (fonte, partição) já gravado na Landing."""
    linhas = _consultar(
        """
        SELECT DISTINCT ON (nm_fonte, ds_particao)
               nm_fonte AS fonte, ds_particao AS particao, ds_chave AS chave, cd_sha256 AS sha256, qt_bytes AS bytes,
               cd_etag_origem AS origem_etag, ds_last_modified_origem AS origem_last_modified, dh_ingestao AS ingerido_em
        FROM controle.tb_arquivo_landing
        ORDER BY nm_fonte, ds_particao, dh_ingestao DESC
        """,
        settings=settings,
    )
    return {(r["fonte"], r["particao"]): r for r in linhas}


def registrar_landing(
    *, chave: str, fonte: str, particao: str, url_origem: str, origem: dict, bytes_: int, sha256: str,
    ingestion_date: str, dag_run_id: str | None, settings: Settings | None = None,
) -> None:
    """Registra (ou atualiza, se o objeto do dia foi regravado) um arquivo da Landing."""
    _executar(
        """
        INSERT INTO controle.tb_arquivo_landing
            (ds_chave, nm_fonte, ds_particao, ds_url_origem, cd_etag_origem, ds_last_modified_origem, qt_bytes,
             cd_sha256, dt_ingestao, dh_ingestao, dh_verificacao, cd_dag_run)
        VALUES (%(chave)s, %(fonte)s, %(particao)s, %(url)s, %(etag)s, %(lm)s, %(bytes)s, %(sha)s,
                %(data)s, now(), now(), %(run)s)
        ON CONFLICT (ds_chave) DO UPDATE SET
            cd_etag_origem = EXCLUDED.cd_etag_origem, ds_last_modified_origem = EXCLUDED.ds_last_modified_origem,
            qt_bytes = EXCLUDED.qt_bytes, cd_sha256 = EXCLUDED.cd_sha256, dh_ingestao = now(), dh_verificacao = now(),
            cd_dag_run = EXCLUDED.cd_dag_run
        """,
        {"chave": chave, "fonte": fonte, "particao": particao, "url": url_origem, "etag": origem.get("etag"),
         "lm": origem.get("last_modified"), "bytes": bytes_, "sha": sha256, "data": ingestion_date, "run": dag_run_id},
        settings,
    )


def atualizar_assinatura_origem(chave: str, origem: dict, settings: Settings | None = None) -> None:
    """A ANS regravou o arquivo com o mesmo conteúdo: guarda a nova assinatura para não
    baixá-lo de novo na próxima noite, sem criar um novo objeto na Landing."""
    _executar(
        """
        UPDATE controle.tb_arquivo_landing
        SET cd_etag_origem = %(etag)s, ds_last_modified_origem = %(lm)s, dh_verificacao = now()
        WHERE ds_chave = %(chave)s
        """,
        {"chave": chave, "etag": origem.get("etag"), "lm": origem.get("last_modified")},
        settings,
    )


# ---------------------------------------------------------------------- Bronze
def pendentes_bronze(fontes: list[str], reprocessar: bool = False, settings: Settings | None = None) -> list[dict]:
    """Arquivo mais recente de cada partição da Landing que a Bronze ainda não converteu."""
    return _consultar(
        """
        WITH ultimo AS (
            SELECT DISTINCT ON (nm_fonte, ds_particao) *
            FROM controle.tb_arquivo_landing
            WHERE nm_fonte = ANY(%(fontes)s)
            ORDER BY nm_fonte, ds_particao, dh_ingestao DESC
        )
        SELECT u.ds_chave AS chave, u.nm_fonte AS fonte, u.ds_particao AS particao,
               u.dt_ingestao::text AS ingestion_date, u.qt_bytes AS bytes,
               to_char(u.dh_ingestao AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS') AS ingerido_em
        FROM ultimo u
        WHERE %(reprocessar)s
           OR NOT EXISTS (
                SELECT 1 FROM controle.tb_particao_bronze b
                WHERE b.ds_chave_landing = u.ds_chave AND b.dh_processamento >= u.dh_ingestao)
        ORDER BY u.nm_fonte, u.ds_particao
        """,
        {"fontes": list(fontes), "reprocessar": bool(reprocessar)},
        settings,
    )


def registrar_bronze(
    *, entidade: str, particao: str, uri: str, chave_landing: str, linhas: int, colunas: int,
    dag_run_id: str | None, settings: Settings | None = None,
) -> None:
    _executar(
        """
        INSERT INTO controle.tb_particao_bronze
            (nm_entidade, ds_particao, ds_uri, ds_chave_landing, qt_linhas, qt_colunas, dh_processamento, cd_dag_run)
        VALUES (%(ent)s, %(part)s, %(uri)s, %(chave)s, %(linhas)s, %(colunas)s, now(), %(run)s)
        ON CONFLICT (nm_entidade, ds_particao) DO UPDATE SET
            ds_uri = EXCLUDED.ds_uri, ds_chave_landing = EXCLUDED.ds_chave_landing, qt_linhas = EXCLUDED.qt_linhas,
            qt_colunas = EXCLUDED.qt_colunas, dh_processamento = now(), cd_dag_run = EXCLUDED.cd_dag_run
        """,
        {"ent": entidade, "part": particao, "uri": uri, "chave": chave_landing, "linhas": linhas,
         "colunas": colunas, "run": dag_run_id},
        settings,
    )


# ---------------------------------------------------------------------- Silver
def pendentes_silver(entidades: list[str], reprocessar: bool = False, settings: Settings | None = None) -> list[dict]:
    """Partições da Bronze cuja versão atual a Silver ainda não processou, com a linhagem
    até o arquivo original na Landing."""
    return _consultar(
        """
        SELECT b.nm_entidade AS entidade, b.ds_particao AS particao, b.ds_uri AS uri, b.ds_chave_landing AS chave_landing,
               b.dh_processamento::text AS bronze_processado_em, l.dt_ingestao::text AS ingestion_date
        FROM controle.tb_particao_bronze b
        JOIN controle.tb_arquivo_landing l ON l.ds_chave = b.ds_chave_landing
        WHERE b.nm_entidade = ANY(%(entidades)s)
          AND (%(reprocessar)s
               OR NOT EXISTS (
                    SELECT 1 FROM controle.tb_particao_silver s
                    WHERE s.nm_entidade = b.nm_entidade AND s.ds_particao = b.ds_particao
                      AND s.dh_processamento_bronze >= b.dh_processamento))
        ORDER BY b.nm_entidade, b.ds_particao
        """,
        {"entidades": list(entidades), "reprocessar": bool(reprocessar)},
        settings,
    )


def registrar_silver(
    *, entidade: str, particao: str, uri: str, bronze_processado_em: str, linhas_validas: int,
    linhas_rejeitadas: int, dag_run_id: str | None, settings: Settings | None = None,
) -> None:
    _executar(
        """
        INSERT INTO controle.tb_particao_silver
            (nm_entidade, ds_particao, ds_uri, dh_processamento_bronze, qt_linhas_validas, qt_linhas_rejeitadas,
             dh_processamento, cd_dag_run)
        VALUES (%(ent)s, %(part)s, %(uri)s, %(bronze)s, %(val)s, %(rej)s, now(), %(run)s)
        ON CONFLICT (nm_entidade, ds_particao) DO UPDATE SET
            ds_uri = EXCLUDED.ds_uri, dh_processamento_bronze = EXCLUDED.dh_processamento_bronze,
            qt_linhas_validas = EXCLUDED.qt_linhas_validas, qt_linhas_rejeitadas = EXCLUDED.qt_linhas_rejeitadas,
            dh_processamento = now(), cd_dag_run = EXCLUDED.cd_dag_run
        """,
        {"ent": entidade, "part": particao, "uri": uri, "bronze": bronze_processado_em, "val": linhas_validas,
         "rej": linhas_rejeitadas, "run": dag_run_id},
        settings,
    )


def particoes_silver(settings: Settings | None = None) -> list[dict]:
    return _consultar(
        "SELECT nm_entidade AS entidade, ds_particao AS particao, ds_uri AS uri FROM controle.tb_particao_silver "
        "ORDER BY nm_entidade, ds_particao",
        settings=settings)


# ---------------------------------------------------------------------- Gold
def _iso(ts: datetime) -> str:
    return ts.isoformat()


def pendentes_gold(reprocessar: bool = False, settings: Settings | None = None) -> dict:
    """Unidades da Gold com partições da Silver mais novas que a última carga.

    Devolve as dimensões (o snapshot mais recente de cada uma), as competências de
    beneficiários e os trimestres financeiros a recarregar, cada um com ``ate``: o
    instante da partição mais nova consumida (vira a marca d'água da carga).
    """
    silver = _consultar(
        "SELECT nm_entidade AS entidade, ds_particao AS particao, ds_uri AS uri, dh_processamento AS processado_em "
        "FROM controle.tb_particao_silver", settings=settings)
    cargas = {r["unidade"]: r["silver_processado_ate"]
              for r in _consultar("SELECT cd_unidade AS unidade, dh_processamento_silver AS silver_processado_ate "
                                  "FROM controle.tb_carga_gold", settings=settings)}

    def pendente(unidade: str, ate: datetime) -> bool:
        return reprocessar or unidade not in cargas or ate > cargas[unidade]

    resultado: dict = {"dm_operadora": None, "dm_produto": None, "competencias": [], "trimestres": []}
    for entidade, unidade in DIMENSOES.items():
        snapshots = [r for r in silver if r["entidade"] == entidade]
        if snapshots:
            ultimo = max(snapshots, key=lambda r: r["processado_em"])
            if pendente(unidade, ultimo["processado_em"]):
                resultado[unidade] = {"uri": ultimo["uri"], "particao": ultimo["particao"],
                                      "ate": _iso(ultimo["processado_em"])}

    grupos: dict[str, list[dict]] = defaultdict(list)
    for r in silver:
        if r["entidade"] == "beneficiarios":
            grupos["c:" + r["particao"].split("/")[0].split("=")[1]].append(r)
        elif r["entidade"] == "financeiro":
            ano, tri = (p.split("=")[1] for p in r["particao"].split("/"))
            grupos[f"t:{ano}-{tri}"].append(r)
    for chave, linhas in sorted(grupos.items()):
        ate = max(r["processado_em"] for r in linhas)
        if chave.startswith("c:"):
            competencia = f"{chave[2:6]}-{chave[6:8]}"
            if pendente(f"ft_beneficiarios:{competencia}", ate):
                resultado["competencias"].append({"competencia": competencia, "ufs": len(linhas), "ate": _iso(ate)})
        else:
            ano, tri = (int(x) for x in chave[2:].split("-"))
            if pendente(f"ft_financeiro:{ano}-T{tri}", ate):
                resultado["trimestres"].append({"ano": ano, "trimestre": tri, "ate": _iso(ate)})
    return resultado


def registrar_carga_gold(
    unidade: str, silver_processado_ate: str, detalhes: dict, dag_run_id: str | None,
    settings: Settings | None = None,
) -> None:
    _executar(
        """
        INSERT INTO controle.tb_carga_gold (cd_unidade, dh_processamento_silver, dh_carga, ds_detalhes, cd_dag_run)
        VALUES (%(u)s, %(ate)s, now(), %(det)s, %(run)s)
        ON CONFLICT (cd_unidade) DO UPDATE SET
            dh_processamento_silver = EXCLUDED.dh_processamento_silver, dh_carga = now(),
            ds_detalhes = EXCLUDED.ds_detalhes, cd_dag_run = EXCLUDED.cd_dag_run
        """,
        {"u": unidade, "ate": silver_processado_ate, "det": json.dumps(detalhes, default=str), "run": dag_run_id},
        settings,
    )
