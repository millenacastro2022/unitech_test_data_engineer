"""Ponte entre as tarefas geradas pela fábrica e o pacote de processamento (src/ans_pipeline).

Toda camada em lotes responde às mesmas três perguntas, com a mesma assinatura:
  listar(grupo, contexto)    -> itens pendentes do grupo (arquivo ou lote de partições)
  processar(item, contexto)  -> resumo do que foi gravado
  rotulo(item)               -> nome do item na UI do Airflow (map_index_template)

Os imports do ans_pipeline ficam dentro das funções: o parse da DAG não carrega Spark nem boto3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

import pendulum

FUSO = "America/Sao_Paulo"


@dataclass(frozen=True)
class Camada:
    listar: Callable[[str, Any], list]
    processar: Callable[[Any, Any], dict]
    rotulo: Callable[[Any], str]


def _data_referencia(contexto) -> "pendulum.Date":
    """Dia da execução em Brasília (execução manual não tem logical_date no Airflow 3)."""
    momento = contexto.get("logical_date") or contexto["dag_run"].run_after
    return pendulum.instance(momento).in_timezone(FUSO).date()


def _rotulo_lote(lote: list[dict]) -> str:
    periodo = lote[0]["particao"].split("/uf=")[0]
    return f"{periodo} · {len(lote)} partição" + ("ões" if len(lote) > 1 else "")


# ---------------------------------------------------------------- Landing: um item = um arquivo
def _listar_landing(fonte: str, contexto) -> list[dict]:
    from ans_pipeline import landing

    return landing.listar_arquivos_novos(_data_referencia(contexto), contexto["params"]["ufs"], fontes=[fonte])


def _baixar(arquivo: dict, contexto) -> dict:
    from ans_pipeline import landing

    return landing.baixar_e_registrar(arquivo, contexto["run_id"])


# ---------------------------------------------------------------- Bronze: um item = um lote
def _listar_bronze(fonte: str, contexto) -> list[list[dict]]:
    from ans_pipeline import bronze

    return bronze.listar_lotes(contexto["params"]["reprocessar"], fontes=[fonte])


def _converter(lote: list[dict], contexto) -> dict:
    from ans_pipeline import bronze

    return bronze.processar_lote(lote, contexto["params"]["reprocessar"], contexto["run_id"])


# ---------------------------------------------------------------- Silver: um item = um lote
def _listar_silver(entidade: str, contexto) -> list[list[dict]]:
    from ans_pipeline import silver

    return silver.listar_lotes(contexto["params"]["reprocessar"], entidades=[entidade])


def _tratar(lote: list[dict], contexto) -> dict:
    from ans_pipeline import silver

    return silver.processar_lote(lote, contexto["params"]["reprocessar"], contexto["run_id"])


CAMADAS: dict[str, Camada] = {
    # rótulo do arquivo: "beneficiarios competencia=202609/uf=SP" -> "competencia=202609/uf=SP"
    "landing": Camada(_listar_landing, _baixar, lambda arquivo: arquivo["rotulo"].split(" ", 1)[-1]),
    "bronze": Camada(_listar_bronze, _converter, _rotulo_lote),
    "silver": Camada(_listar_silver, _tratar, _rotulo_lote),
}
