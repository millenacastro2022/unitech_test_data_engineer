"""Camada Landing: baixa os arquivos da ANS e os grava *sem nenhuma alteração* no
Object Storage, organizados por data de ingestão. Antes de baixar, compara a assinatura
da publicação na ANS (ETag, data e tamanho) com a do último arquivo da partição, para só
baixar o que é novo ou foi republicado.

Layout:  s3://landing/<fonte>/ingestion_date=AAAA-MM-DD/[<particao>/]<arquivo original>
Ex.:     s3://landing/beneficiarios/ingestion_date=2026-11-07/competencia=202609/pda-024-icb-SP-2026_09.zip
"""

from __future__ import annotations

import hashlib
import logging
import tempfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from ans_pipeline import storage
from ans_pipeline.config import MESES_RETENCAO, TRIMESTRES_RETENCAO, Settings, get_settings
from ans_pipeline.fontes import ArquivoFonte

log = logging.getLogger(__name__)

USER_AGENT = "ans-lakehouse-pipeline/1.0"


class ArquivoNaoPublicado(Exception):
    """A ANS ainda não publicou o arquivo (HTTP 404)."""


@dataclass(frozen=True)
class RefLanding:
    """Referência a um objeto gravado na Landing (trafega entre tasks via XCom)."""

    fonte: str
    bucket: str
    key: str
    url_origem: str
    ingestion_date: str
    sha256: str
    bytes: int

    def to_dict(self) -> dict:
        return asdict(self)


def _sessao_http() -> requests.Session:
    sessao = requests.Session()
    # Retentativas para falhas transitórias de rede/servidor (o Airflow ainda faz retries da task).
    retry = Retry(
        total=4, backoff_factor=2, status_forcelist=(429, 500, 502, 503, 504), allowed_methods=("GET", "HEAD")
    )
    sessao.mount("https://", HTTPAdapter(max_retries=retry))
    sessao.mount("http://", HTTPAdapter(max_retries=retry))
    sessao.headers["User-Agent"] = USER_AGENT
    return sessao


def assinatura_publicacao(url: str, settings: Settings | None = None, sessao: requests.Session | None = None) -> dict | None:
    """Assinatura do arquivo publicado na ANS (HEAD, sem baixar), ou None se não publicado."""
    s = settings or get_settings()
    resp = (sessao or _sessao_http()).head(url, timeout=(30, s.http_timeout_seconds), allow_redirects=True)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return {
        "etag": resp.headers.get("ETag"),
        "last_modified": resp.headers.get("Last-Modified"),
        "bytes": int(resp.headers.get("Content-Length") or 0),
    }


def mesma_publicacao(atual: dict, anterior: dict | None) -> bool:
    """O arquivo da ANS é o mesmo já ingerido? Compara ETag; sem ele, data e tamanho."""
    if not anterior:
        return False
    if atual.get("etag") and anterior.get("origem_etag"):
        return atual["etag"] == anterior["origem_etag"]
    return (atual.get("last_modified"), atual.get("bytes")) == (anterior.get("origem_last_modified"), anterior.get("bytes"))


def listar_novidades(
    arquivos: list[ArquivoFonte], ultimos: dict[tuple[str, str], dict], settings: Settings | None = None
) -> tuple[list[dict], dict[str, int]]:
    """Consulta a ANS e devolve os arquivos publicados que são novos ou mudaram.

    ``ultimos`` é o arquivo mais recente de cada (fonte, partição) já na Landing
    (``controle.ultimos_landing``). Devolve também a contagem por situação, para o log.
    """
    s = settings or get_settings()
    sessao = _sessao_http()
    with ThreadPoolExecutor(max_workers=8) as pool:
        assinaturas = list(pool.map(lambda a: assinatura_publicacao(a.url, s, sessao), arquivos))

    novidades: list[dict] = []
    contagem = {"nao_publicado": 0, "sem_mudanca": 0, "novo": 0, "atualizado": 0}
    for arquivo, assinatura in zip(arquivos, assinaturas, strict=True):
        anterior = ultimos.get((arquivo.fonte, arquivo.particao_controle))
        if assinatura is None:
            contagem["nao_publicado"] += 1
        elif mesma_publicacao(assinatura, anterior):
            contagem["sem_mudanca"] += 1
        else:
            situacao = "atualizado" if anterior else "novo"
            contagem[situacao] += 1
            novidades.append({
                **arquivo.to_dict(), "rotulo": arquivo.rotulo, "situacao": situacao, "origem": assinatura,
                "chave_anterior": anterior["chave"] if anterior else None,
                "sha256_anterior": anterior["sha256"].strip() if anterior else None,
            })
    log.info("Verificados %d arquivos na ANS: %s", len(arquivos), contagem)
    return novidades, contagem


def baixar(url: str, destino: Path, settings: Settings | None = None) -> tuple[str, int, str | None]:
    """Download em streaming. Retorna (sha256, bytes, last-modified)."""
    s = settings or get_settings()
    sha = hashlib.sha256()
    total = 0
    with _sessao_http().get(url, stream=True, timeout=(30, s.http_timeout_seconds)) as resp:
        if resp.status_code == 404:
            raise ArquivoNaoPublicado(url)
        resp.raise_for_status()
        esperado = int(resp.headers.get("Content-Length", 0) or 0)
        with open(destino, "wb") as f:
            for bloco in resp.iter_content(chunk_size=1024 * 1024):
                f.write(bloco)
                sha.update(bloco)
                total += len(bloco)
        last_modified = resp.headers.get("Last-Modified")
    if esperado and total != esperado:
        raise IOError(f"Download incompleto de {url}: {total} de {esperado} bytes")
    if total == 0:
        raise IOError(f"Arquivo vazio: {url}")
    return sha.hexdigest(), total, last_modified


def chave_landing(arquivo: ArquivoFonte, ingestion_date: date) -> str:
    partes = [arquivo.fonte, f"ingestion_date={ingestion_date.isoformat()}"]
    if arquivo.particao:
        partes.append(arquivo.particao)
    partes.append(arquivo.nome_arquivo)
    return "/".join(partes)


def ingerir(
    arquivo: ArquivoFonte,
    *,
    ingestion_date: date | None = None,
    sha256_anterior: str | None = None,
    contexto: dict[str, str] | None = None,
    settings: Settings | None = None,
) -> RefLanding | None:
    """Baixa ``arquivo`` da ANS e grava na Landing.

    Se o conteúdo baixado for idêntico ao último ingerido (``sha256_anterior``), nada é
    gravado e a função devolve None: a ANS às vezes regrava um arquivo sem alterá-lo.
    """
    s = settings or get_settings()
    ingestion_date = ingestion_date or datetime.now(timezone.utc).date()
    key = chave_landing(arquivo, ingestion_date)

    with tempfile.TemporaryDirectory(prefix="landing_") as tmp:
        local = Path(tmp) / arquivo.nome_arquivo
        log.info("Baixando %s", arquivo.url)
        sha256, tamanho, last_modified = baixar(arquivo.url, local, s)
        if sha256_anterior and sha256 == sha256_anterior:
            log.info("Conteúdo idêntico ao último ingerido (sha256=%s): nada gravado", sha256[:12])
            return None
        metadados = {
            "source-url": arquivo.url,
            "sha256": sha256,
            "source-last-modified": last_modified or "",
            "ingested-at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            **(contexto or {}),
        }
        storage.enviar_arquivo(local, s.landing_bucket, key, metadados, s)

    log.info("Landing: s3://%s/%s (%.1f MB, sha256=%s)", s.landing_bucket, key, tamanho / 1e6, sha256[:12])
    return RefLanding(
        fonte=arquivo.fonte, bucket=s.landing_bucket, key=key, url_origem=arquivo.url,
        ingestion_date=ingestion_date.isoformat(), sha256=sha256, bytes=tamanho,
    )


# ---------------------------------------------------------------------- o que a DAG chama
# A ANS publica cada competência semanas depois do fim do mês (e o DIOPS meses depois do
# trimestre). Por isso a Landing consulta alguns períodos a mais do que a retenção guarda e
# fica com os mais recentes que já foram publicados.
MESES_VERIFICADOS = MESES_RETENCAO + 3
TRIMESTRES_VERIFICADOS = TRIMESTRES_RETENCAO + 2


def listar_arquivos_novos(
    referencia: date,
    ufs: list[str] | None = None,
    settings: Settings | None = None,
    fontes: list[str] | None = None,
) -> list[dict]:
    """Arquivos publicados pela ANS que são novos ou mudaram desde a última ingestão,
    só dentro da janela de retenção. ``fontes`` restringe a consulta (padrão: todas).

    Consulta os últimos ``MESES_VERIFICADOS`` meses de beneficiários, os últimos
    ``TRIMESTRES_VERIFICADOS`` trimestres do DIOPS e os snapshots de operadoras e produtos;
    devolve só o que cai nas ``MESES_RETENCAO`` competências e nos ``TRIMESTRES_RETENCAO``
    trimestres mais recentes (veja ``ans_pipeline.retencao``).
    """
    from ans_pipeline import controle, retencao
    from ans_pipeline import fontes as catalogo
    from ans_pipeline.config import UFS
    from ans_pipeline.periodo import janela_competencias, trimestres_fechados

    s = settings or get_settings()
    storage.garantir_buckets(s)
    controle.aplicar_ddl(s)
    competencias = janela_competencias(referencia, MESES_VERIFICADOS)
    trimestres = trimestres_fechados(referencia, TRIMESTRES_VERIFICADOS)
    ufs = sorted({u.strip().upper() for u in (ufs or UFS)})
    if set(ufs) - set(UFS):
        raise ValueError(f"UFs inválidas: {sorted(set(ufs) - set(UFS))}")

    alvo = list(fontes or catalogo.FONTES)
    if set(alvo) - set(catalogo.FONTES):
        raise ValueError(f"Fontes inválidas: {sorted(set(alvo) - set(catalogo.FONTES))}")
    candidatos = catalogo.candidatos(s.ans_base_url, competencias, trimestres, ufs, alvo)
    log.info("Verificando %d arquivos: competências %s, trimestres %s", len(candidatos),
             [c.iso for c in competencias], trimestres)
    ultimos = controle.ultimos_landing(s)
    novidades, _ = listar_novidades(candidatos, ultimos, s)
    manter, fora, retidos = retencao.filtrar_novidades(novidades, ultimos)
    if fora:
        por_periodo: dict[str, int] = {}
        for n in fora:
            chave = f"{n['fonte']} {retencao.periodo(n['fonte'], n['particao'])}"
            por_periodo[chave] = por_periodo.get(chave, 0) + 1
        log.info("Fora da janela de retenção, não baixados (arquivos por período): %s", dict(sorted(por_periodo.items())))
    log.info("Janela de retenção: %s", {f: sorted(p) for f, p in retidos.items()})
    return manter


def baixar_e_registrar(arquivo: dict, dag_run_id: str | None = None, settings: Settings | None = None) -> dict:
    """Baixa um arquivo listado por ``listar_arquivos_novos``, grava na Landing e registra no controle."""
    from ans_pipeline import controle

    s = settings or get_settings()
    fonte = ArquivoFonte.from_dict(arquivo)
    metadados = {"run-id": dag_run_id or "", "source-etag": arquivo["origem"].get("etag") or ""}
    ref = ingerir(fonte, sha256_anterior=arquivo.get("sha256_anterior"), contexto=metadados, settings=s)
    if ref is None:  # a ANS regravou o arquivo sem mudar o conteúdo
        controle.atualizar_assinatura_origem(arquivo["chave_anterior"], arquivo["origem"], s)
        return {"arquivo": arquivo["rotulo"], "resultado": "conteúdo idêntico, nada gravado"}
    controle.registrar_landing(
        chave=ref.key, fonte=fonte.fonte, particao=fonte.particao_controle, url_origem=fonte.url,
        origem=arquivo["origem"], bytes_=ref.bytes, sha256=ref.sha256, ingestion_date=ref.ingestion_date,
        dag_run_id=dag_run_id, settings=s,
    )
    return {"arquivo": arquivo["rotulo"], "resultado": arquivo["situacao"], "chave": ref.key, "bytes": ref.bytes}
