"""Acesso ao Object Storage (RustFS via API S3)."""

from __future__ import annotations

import logging
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from ans_pipeline.config import Settings, get_settings

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------- S3 / boto3
def s3_client(settings: Settings | None = None):
    s = settings or get_settings()
    return boto3.client(
        "s3",
        endpoint_url=s.s3_endpoint_url,
        aws_access_key_id=s.s3_access_key,
        aws_secret_access_key=s.s3_secret_key,
        region_name=s.s3_region,
        config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5, "mode": "standard"}),
    )


def garantir_buckets(settings: Settings | None = None) -> list[str]:
    """Cria os buckets das camadas Landing, Bronze e Silver, se ainda não existirem (idempotente)."""
    s = settings or get_settings()
    client = s3_client(s)
    existentes = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    for bucket in (s.landing_bucket, s.bronze_bucket, s.silver_bucket):
        if bucket not in existentes:
            client.create_bucket(Bucket=bucket)
            log.info("Bucket criado: %s", bucket)
    return [s.landing_bucket, s.bronze_bucket, s.silver_bucket]


def objeto_existe(bucket: str, key: str, settings: Settings | None = None) -> bool:
    try:
        s3_client(settings).head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def metadados_objeto(bucket: str, key: str, settings: Settings | None = None) -> dict:
    resp = s3_client(settings).head_object(Bucket=bucket, Key=key)
    return {"bytes": resp["ContentLength"], **resp.get("Metadata", {})}


def enviar_arquivo(
    caminho: Path, bucket: str, key: str, metadados: dict[str, str], settings: Settings | None = None
) -> None:
    # Metadados S3 aceitam apenas ASCII.
    meta = {k: str(v).encode("ascii", "ignore").decode() for k, v in metadados.items()}
    s3_client(settings).upload_file(str(caminho), bucket, key, ExtraArgs={"Metadata": meta})


def baixar_arquivo(bucket: str, key: str, destino: Path, settings: Settings | None = None) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    s3_client(settings).download_file(bucket, key, str(destino))
    return destino


def listar_chaves(bucket: str, prefixo: str, settings: Settings | None = None) -> list[str]:
    paginator = s3_client(settings).get_paginator("list_objects_v2")
    chaves: list[str] = []
    for pagina in paginator.paginate(Bucket=bucket, Prefix=prefixo):
        chaves.extend(o["Key"] for o in pagina.get("Contents", []))
    return chaves


def baixar_objetos(bucket: str, chaves: list[str], destino: Path, settings: Settings | None = None) -> list[Path]:
    """Baixa vários objetos para ``destino``, com nomes planos (sem as pastas ``chave=valor``,
    para o Spark não inferir partições a partir do caminho)."""
    destino.mkdir(parents=True, exist_ok=True)
    cliente = s3_client(settings)
    arquivos = []
    for chave in chaves:
        local = destino / chave.replace("/", "__").replace("=", "-")
        cliente.download_file(bucket, chave, str(local))
        arquivos.append(local)
    return arquivos
