"""Configuração do pipeline, lida de variáveis de ambiente (definidas no docker-compose)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Raiz dos scripts SQL (montada em /opt/airflow/sql no container).
SQL_DIR = Path(os.getenv("ANS_SQL_DIR", Path(__file__).resolve().parents[2] / "sql"))

# 26 UFs + DF + "XX" (arquivo da ANS com beneficiários de UF não identificada).
UFS: tuple[str, ...] = (
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO", "XX",
)

# Retenção: quantos períodos mais recentes o lakehouse guarda (veja ans_pipeline.retencao).
MESES_RETENCAO = int(os.getenv("ANS_MESES_RETENCAO", "3"))          # competências de beneficiários
TRIMESTRES_RETENCAO = int(os.getenv("ANS_TRIMESTRES_RETENCAO", "1"))  # trimestres do DIOPS


@dataclass(frozen=True)
class Settings:
    ans_base_url: str
    s3_endpoint_url: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    landing_bucket: str
    bronze_bucket: str
    silver_bucket: str
    dw_host: str
    dw_port: int
    dw_db: str
    dw_user: str
    dw_password: str
    spark_driver_memory: str
    spark_cores: int
    spark_jdbc_jar: str | None
    http_timeout_seconds: int

    @property
    def dw_jdbc_url(self) -> str:
        """URL JDBC do DW, usada pelo Spark para publicar a Silver no schema silver."""
        return f"jdbc:postgresql://{self.dw_host}:{self.dw_port}/{self.dw_db}"

    @property
    def dw_dsn(self) -> str:
        """DSN no formato libpq (psycopg2)."""
        return (
            f"host={self.dw_host} port={self.dw_port} dbname={self.dw_db} "
            f"user={self.dw_user} password={self.dw_password}"
        )



def get_settings() -> Settings:
    env = os.getenv
    return Settings(
        ans_base_url=env("ANS_BASE_URL", "https://dadosabertos.ans.gov.br/FTP/PDA").rstrip("/"),
        s3_endpoint_url=env("S3_ENDPOINT_URL", "http://localhost:9000"),
        s3_access_key=env("S3_ACCESS_KEY", "rustfsadmin"),
        s3_secret_key=env("S3_SECRET_KEY", "rustfsadmin123"),
        s3_region=env("S3_REGION", "us-east-1"),
        landing_bucket=env("LANDING_BUCKET", "landing"),
        bronze_bucket=env("BRONZE_BUCKET", "bronze"),
        silver_bucket=env("SILVER_BUCKET", "silver"),
        dw_host=env("DW_HOST", "localhost"),
        dw_port=int(env("DW_PORT", "5433")),
        dw_db=env("DW_DB", "ans_dw"),
        dw_user=env("DW_USER", "dw"),
        dw_password=env("DW_PASSWORD", "dw"),
        spark_driver_memory=env("SPARK_DRIVER_MEMORY", "2g"),
        spark_cores=int(env("SPARK_CORES", "4")),
        spark_jdbc_jar=env("SPARK_JDBC_JAR"),
        http_timeout_seconds=int(env("HTTP_TIMEOUT_SECONDS", "300")),
    )
