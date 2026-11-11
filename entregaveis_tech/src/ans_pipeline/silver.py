"""Camada Silver: lê a tabela da Bronze (Parquet, tudo como texto), aplica limpeza, tipagem e
deduplicação com PySpark (regras em ``regras_silver.py``) e grava Parquet (zstd) no Object Storage.

Linhas inválidas não são descartadas silenciosamente: vão para
``s3://silver/_quarentena/tb_<entidade>/...`` com a coluna ``ds_motivo_rejeicao``.
"""

from __future__ import annotations

import logging
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from ans_pipeline import quality, storage
from ans_pipeline.config import Settings, get_settings
from ans_pipeline.entidades import ENTIDADES, Entidade, tabela_silver
from ans_pipeline.periodo import Competencia

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

MOTIVO = "ds_motivo_rejeicao"


@dataclass
class ResultadoSilver:
    entidade: str
    uri: str
    uri_quarentena: str | None
    linhas_lidas: int
    linhas_validas: int
    linhas_rejeitadas: int
    duplicatas_removidas: int

    def to_dict(self) -> dict:
        return asdict(self)


def parametros_da_particao(particao: str) -> dict:
    """Período esperado no arquivo, conferido pelas regras: competência ou início do trimestre."""
    partes = dict(p.split("=", 1) for p in particao.split("/") if "=" in p)
    if "competencia" in partes:
        return {"competencia": Competencia.parse(partes["competencia"]).primeiro_dia.isoformat()}
    if "trimestre" in partes:
        ano, tri = int(partes["ano"]), int(partes["trimestre"])
        return {"dt_trimestre": Competencia(ano, (tri - 1) * 3 + 1).primeiro_dia.isoformat()}
    return {}


def transformar(
    spark: "SparkSession", entidade: Entidade, bronze: "DataFrame", parametros: dict, trabalho: Path | None = None
) -> tuple["DataFrame", dict[str, int]]:
    """Aplica a regra da entidade à tabela da Bronze e devolve (linhas tratadas, métricas).
    As tratadas incluem as rejeitadas, com ``ds_motivo_rejeicao`` preenchido. Independente de
    S3 (é o que os testes usam). Com ``trabalho``, o resultado é materializado em Parquet
    local e as contagens e gravações seguintes leem o resultado pronto."""
    from pyspark.sql import functions as F

    from ans_pipeline.regras_silver import REGRAS

    tratado = REGRAS[entidade.nome](bronze, parametros)
    if trabalho is not None:
        tratado.write.mode("overwrite").parquet(str(trabalho / "tratado"))
        tratado = spark.read.parquet(str(trabalho / "tratado"))

    lidas = bronze.count()
    validas, rejeitadas = tratado.agg(
        F.count_if(F.col(MOTIVO).isNull()), F.count_if(F.col(MOTIVO).isNotNull())
    ).first()
    return tratado, {
        "linhas_lidas": lidas,
        "linhas_validas": validas,
        "linhas_rejeitadas": rejeitadas,
        "duplicatas_removidas": lidas - validas - rejeitadas,
    }


# ---------------------------------------------------------------------- uma partição
def processar(spark: "SparkSession", item: dict, dag_run_id: str | None = None,
              settings: Settings | None = None) -> dict:
    """Bronze (Parquet bruto) -> Silver (Parquet tratado) para uma partição, e registro no controle.
    A escrita sobrescreve a partição, portanto reprocessar é idempotente."""
    from pyspark.sql import functions as F

    from ans_pipeline import controle
    from ans_pipeline.spark import gravar_parquet_unico

    s = settings or get_settings()
    entidade = ENTIDADES[item["entidade"]]
    particao = item["particao"]
    tabela = tabela_silver(entidade.nome)  # tb_<entidade>
    chave = f"{tabela}/{particao}/{tabela}.parquet"
    chave_quarentena = f"_quarentena/{tabela}/{particao}/rejeitados.parquet"
    params = {
        "ingestion_date": item["ingestion_date"],
        "arquivo_origem": f"s3://{s.landing_bucket}/{item['chave_landing']}",
        **parametros_da_particao(particao),
    }

    with tempfile.TemporaryDirectory(prefix=f"silver_{entidade.nome}_") as tmp:
        tmp_path = Path(tmp)
        chave_bronze = item["uri"].split(f"s3://{s.bronze_bucket}/", 1)[1]
        local = storage.baixar_arquivo(s.bronze_bucket, chave_bronze, tmp_path / "bronze" / f"{entidade.nome}.parquet", s)
        bronze = spark.read.parquet(str(local))

        tratado, metricas = transformar(spark, entidade, bronze, params, trabalho=tmp_path / "spark")
        # Qualidade é verificada ANTES de gravar: dado reprovado não chega à Silver.
        quality.validar_silver(tratado, entidade, metricas)

        validas = tratado.filter(F.col(MOTIVO).isNull()).drop(MOTIVO)
        arquivo = gravar_parquet_unico(validas, tmp_path / "saida", entidade.ordenacao)
        storage.enviar_arquivo(arquivo, s.silver_bucket, chave, {"source-key": chave_bronze}, s)

        uri_quarentena = None
        if metricas["linhas_rejeitadas"]:
            rejeitadas = tratado.filter(F.col(MOTIVO).isNotNull())
            arquivo_q = gravar_parquet_unico(rejeitadas, tmp_path / "quarentena")
            storage.enviar_arquivo(arquivo_q, s.silver_bucket, chave_quarentena, {"source-key": chave_bronze}, s)
            uri_quarentena = f"s3://{s.silver_bucket}/{chave_quarentena}"
            motivos = [(r[0], r[1]) for r in rejeitadas.groupBy(MOTIVO).count().orderBy(F.desc("count")).collect()]
            log.warning("%s: %d linhas em quarentena %s", entidade.nome, metricas["linhas_rejeitadas"], motivos)
        elif storage.objeto_existe(s.silver_bucket, chave_quarentena, s):
            # Reprocessamento sem rejeições: remove quarentena antiga da mesma partição.
            storage.s3_client(s).delete_object(Bucket=s.silver_bucket, Key=chave_quarentena)

    resultado = ResultadoSilver(entidade=entidade.nome, uri=f"s3://{s.silver_bucket}/{chave}",
                                uri_quarentena=uri_quarentena, **metricas)
    controle.registrar_silver(
        entidade=entidade.nome, particao=particao, uri=resultado.uri, bronze_processado_em=item["bronze_processado_em"],
        linhas_validas=metricas["linhas_validas"], linhas_rejeitadas=metricas["linhas_rejeitadas"],
        dag_run_id=dag_run_id, settings=s,
    )
    log.info("Silver: %s", resultado)
    return {"particao": f"{entidade.nome} {particao}", **resultado.to_dict()}


# ---------------------------------------------------------------------- o que a DAG chama
def listar_lotes(reprocessar: bool = False, settings: Settings | None = None,
                 entidades: list[str] | None = None) -> list[list[dict]]:
    """Partições da Bronze que a Silver ainda não processou, em lotes por entidade e período.
    ``entidades`` restringe a consulta (padrão: todas)."""
    from ans_pipeline import controle
    from ans_pipeline.periodo import agrupar_por_periodo

    s = settings or get_settings()
    controle.aplicar_ddl(s)
    lotes = agrupar_por_periodo(controle.pendentes_silver(list(entidades or ENTIDADES), reprocessar, s), campo="entidade")
    log.info("%d lotes: %s", len(lotes), [f"{l[0]['entidade']} {l[0]['particao'].split('/uf=')[0]} ({len(l)})" for l in lotes])
    return lotes


def processar_lote(lote: list[dict], reprocessar: bool = False, dag_run_id: str | None = None,
                   settings: Settings | None = None) -> dict:
    """Um lote numa única sessão Spark. Numa retentativa, pula o que já foi processado
    (a menos que seja um reprocessamento forçado)."""
    from ans_pipeline import controle
    from ans_pipeline.spark import sessao

    s = settings or get_settings()
    if not reprocessar:
        ainda = {(p["entidade"], p["particao"]) for p in controle.pendentes_silver(sorted({i["entidade"] for i in lote}), settings=s)}
        lote = [i for i in lote if (i["entidade"], i["particao"]) in ainda]
    feitos = []
    if lote:
        with tempfile.TemporaryDirectory(prefix="spark_") as tmp, sessao("silver", s, temp_dir=Path(tmp)) as spark:
            for item in lote:
                r = processar(spark, item, dag_run_id, s)
                feitos.append({k: r[k] for k in ("particao", "linhas_lidas", "linhas_validas", "linhas_rejeitadas")})
    return {"particoes": len(feitos), "linhas_validas": sum(f["linhas_validas"] for f in feitos), "detalhe": feitos}
