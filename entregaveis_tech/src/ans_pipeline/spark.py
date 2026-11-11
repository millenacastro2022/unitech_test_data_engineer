"""Sessão Spark: PySpark em modo local, dentro da própria task do Airflow.

Um nó só não justifica um cluster: o Spark usa os núcleos do container (``SPARK_CORES``) e
grava em disco o que não cabe na memória do driver (``SPARK_DRIVER_MEMORY``). Para rodar
num cluster (Kubernetes, EMR, Databricks), basta trocar o ``master``.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Iterator

from ans_pipeline.config import Settings, get_settings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)


@contextmanager
def sessao(nome: str, settings: Settings | None = None, *, temp_dir: Path | None = None) -> Iterator["SparkSession"]:
    from pyspark.sql import SparkSession

    s = settings or get_settings()
    builder = (
        SparkSession.builder.appName(nome)
        .master(f"local[{s.spark_cores}]")
        .config("spark.driver.memory", s.spark_driver_memory)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(max(4, 2 * s.spark_cores)))
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    )
    if temp_dir is not None:
        temp_dir.mkdir(parents=True, exist_ok=True)
        builder = builder.config("spark.local.dir", str(temp_dir))
    if s.spark_jdbc_jar:
        builder = builder.config("spark.jars", s.spark_jdbc_jar)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark %s: master=%s, driver.memory=%s", spark.version, spark.sparkContext.master, s.spark_driver_memory)
    try:
        yield spark
    finally:
        spark.stop()


def gravar_parquet_unico(df: "DataFrame", destino: Path, ordenacao: tuple[str, ...] = ()) -> Path:
    """Grava ``df`` como um único arquivo Parquet (zstd) e devolve o caminho dele.

    A Silver guarda um arquivo por partição (``<entidade>/<partição>/<entidade>.parquet``);
    o Spark grava uma pasta com ``part-*.parquet``, então o arquivo é escrito localmente e
    depois enviado ao Object Storage com o nome final.
    """
    saida = df.repartition(1)
    if ordenacao:
        saida = saida.sortWithinPartitions(*ordenacao)
    saida.write.mode("overwrite").parquet(str(destino))
    partes = sorted(destino.glob("part-*.parquet"))
    if len(partes) != 1:
        raise IOError(f"Esperava 1 arquivo Parquet em {destino}, encontrei {len(partes)}")
    return partes[0]


def gravar_jdbc(df: "DataFrame", tabela: str, settings: Settings | None = None) -> None:
    """Acrescenta ``df`` a uma tabela já criada no DW (o DDL do schema silver cria as tabelas)."""
    s = settings or get_settings()
    (df.write.format("jdbc")
     .option("url", s.dw_jdbc_url + "?reWriteBatchedInserts=true")
     .option("dbtable", tabela)
     .option("user", s.dw_user)
     .option("password", s.dw_password)
     .option("driver", "org.postgresql.Driver")
     .option("batchsize", 10000)
     .mode("append")
     .save())
