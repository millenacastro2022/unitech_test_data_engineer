"""Fábrica das DAGs da ANS: uma DAG por camada, montada a partir de config/camadas.yaml.

Recursos do Airflow usados para a DAG ser dinâmica e escalável:

- **DAGs geradas por configuração**: horário, grupos, limites e parâmetros vêm do YAML;
  o arquivo de DAG só percorre a lista e registra cada DAG.
- **TaskGroup por fonte/entidade** (o substituto das SubDAGs): cada grupo lista e processa o
  que é seu, sem esperar os outros; a falha de um não interrompe os demais.
- **Dynamic Task Mapping**: uma tarefa por arquivo (Landing) ou por lote de partições
  (Bronze, Silver), criada em tempo de execução com `.expand()`, com o rótulo do item na UI.
- **XCom**: a lista de pendências vai da tarefa que lista para as tarefas mapeadas.
- **Pools**: `spark` limita os jobs PySpark de todas as DAGs juntas; `ans_download`, os
  downloads simultâneos. Escalar é aumentar slots ou workers, sem mexer no código.
- **Branching + ExternalTaskSensor**: a execução agendada espera a camada anterior do mesmo
  dia terminar; a execução manual (reprocessamento) segue direto.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pendulum
import yaml

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import DAG, Param, TaskGroup, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from fabrica_dags.processamento import CAMADAS

CONFIG = Path(__file__).resolve().parents[1] / "config" / "camadas.yaml"
GRUPO_ESPERA = "wait_upstream"


def carregar_config(caminho: Path = CONFIG) -> dict:
    with open(caminho, encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------- parâmetros
def _params(definicoes: dict[str, str]) -> dict[str, Param]:
    """Parâmetros da execução manual. O tipo vem daqui; a descrição, do YAML."""
    from ans_pipeline.config import UFS

    tipos = {
        "ufs": lambda d: Param(list(UFS), type="array", items={"type": "string", "enum": list(UFS)}, description=d),
        "reprocessar": lambda d: Param(False, type="boolean", description=d),
    }
    return {nome: tipos[nome](descricao) for nome, descricao in (definicoes or {}).items()}


def _opcoes(cfg: dict) -> dict:
    """Pool e tempo limite de uma tarefa, do YAML."""
    opcoes = {}
    if cfg.get("pool"):
        opcoes["pool"] = cfg["pool"]
    if cfg.get("limite_min"):
        opcoes["execution_timeout"] = timedelta(minutes=cfg["limite_min"])
    return opcoes


def _minutos_do_dia(cron: str) -> int:
    minuto, hora, *_ = cron.split()
    return int(hora) * 60 + int(minuto)


def _hora(cron: str) -> str:
    """'0 3 * * *' -> '03:00'."""
    return "{:02d}:{:02d}".format(*divmod(_minutos_do_dia(cron), 60))


# ---------------------------------------------------------------------- espera da camada anterior
def espera_camada_anterior(cfg: dict, anterior: dict) -> TaskGroup:
    """Branching + ExternalTaskSensor.

    Execução agendada: espera a execução do mesmo dia da camada anterior terminar (com sucesso
    ou falha: o que ela gravou já está no controle). Se ela não aparecer no prazo, o sensor é
    pulado e a camada segue com o que estiver pendente. Execução manual: segue direto.
    """
    espera = cfg["espera_anterior"]
    sensor_id = f"wait_{anterior['dag_id']}"
    atraso = _minutos_do_dia(cfg["horario"]) - _minutos_do_dia(anterior["horario"])

    with TaskGroup(GRUPO_ESPERA, tooltip=f"Espera a {anterior['dag_id']} do mesmo dia") as grupo:

        @task.branch(task_id="check_run_type")
        def check_run_type(dag_run=None) -> str:
            """Agendada → espera a camada anterior; manual → segue direto."""
            tipo = getattr(dag_run.run_type, "value", dag_run.run_type)
            return f"{GRUPO_ESPERA}.{sensor_id}" if tipo == "scheduled" else f"{GRUPO_ESPERA}.manual_run"

        sensor = ExternalTaskSensor(
            task_id=sensor_id,
            external_dag_id=anterior["dag_id"],
            external_task_id=None,  # a DAG inteira
            allowed_states=["success", "failed"],  # terminou
            execution_delta=timedelta(minutes=atraso),
            check_existence=True,
            mode="reschedule",  # libera o slot entre uma verificação e outra
            poke_interval=espera["verificar_a_cada_min"] * 60,
            timeout=espera["limite_min"] * 60,
            soft_fail=True,  # no prazo estourado, pula em vez de falhar
            doc_md=f"Espera a `{anterior['dag_id']}` agendada {atraso // 60} h antes terminar.",
        )
        manual = EmptyOperator(task_id="manual_run")
        pronto = EmptyOperator(task_id="ready", trigger_rule="none_failed")
        check_run_type() >> [sensor, manual] >> pronto
    return grupo


# ---------------------------------------------------------------------- camadas em lotes
def grupo_da_fonte(camada: str, grupo: str, cfg: dict) -> TaskGroup:
    """TaskGroup de uma fonte/entidade: lista as pendências e processa cada uma (mapeamento)."""
    ponte = CAMADAS[camada]
    listar_cfg, processar_cfg = cfg["listar"], cfg["processar"]

    with TaskGroup(grupo, tooltip=f"{camada} · {grupo}") as tg:

        @task(task_id=listar_cfg["tarefa"], doc_md=listar_cfg.get("descricao"))
        def listar() -> list:
            return ponte.listar(grupo, get_current_context())

        @task(task_id=processar_cfg["tarefa"], doc_md=processar_cfg.get("descricao"),
              map_index_template="{{ rotulo_item }}", **_opcoes(processar_cfg))
        def processar(item) -> dict:
            contexto = get_current_context()
            contexto["rotulo_item"] = ponte.rotulo(item)
            return ponte.processar(item, contexto)

        processar.expand(item=listar())
    return tg


# ---------------------------------------------------------------------- Gold
def etapas_gold(cfg: dict):
    """A Gold é uma sequência: portão → publicação (Spark) → dbt → validação → retenção.
    Devolve a primeira e a última tarefa."""
    etapas = cfg["etapas"]

    @task(task_id="validate_silver_complete", **_opcoes(etapas["validate_silver_complete"]))
    def validate_silver_complete() -> dict:
        """Portão: todo arquivo da Landing na Bronze, toda partição da Bronze na Silver e toda
        partição registrada existente no storage. Falha sem retentativas se faltar algo."""
        from ans_pipeline import quality

        try:
            return quality.validar_silver_completa()
        except quality.FalhaQualidade as erro:
            raise AirflowFailException(str(erro)) from erro

    @task(task_id="publish_silver", **_opcoes(etapas["publish_silver"]))
    def publish_silver(params=None) -> dict:
        """PySpark: publica no schema `silver` do PostgreSQL só o período que mudou na Silver."""
        from ans_pipeline import gold

        return gold.publicar_silver(params["reprocessar"])

    @task(task_id="dbt_build", **_opcoes(etapas["dbt_build"]))
    def dbt_build(publicado: dict) -> dict:
        """Dimensões e fatos incrementais com contrato, chaves e testes do dbt."""
        from ans_pipeline import gold

        try:
            return gold.executar_dbt(publicado)
        except gold.FalhaDbt as erro:
            raise AirflowFailException(str(erro)) from erro

    @task(task_id="validate_gold", **_opcoes(etapas["validate_gold"]))
    def validate_gold(publicado: dict, run_id=None) -> dict:
        """Reconcilia cada competência e trimestre com a Silver e só então registra a carga."""
        from ans_pipeline import gold, quality

        try:
            return gold.validar_cargas(publicado, run_id)
        except quality.FalhaQualidade as erro:
            raise AirflowFailException(str(erro)) from erro

    @task(task_id="apply_retention", **_opcoes(etapas["apply_retention"]))
    def apply_retention() -> dict:
        """Apaga das quatro camadas o que saiu da janela de retenção, com o controle."""
        from ans_pipeline import retencao

        return retencao.expurgar()

    portao = validate_silver_complete()
    publicado = publish_silver()
    fim = apply_retention()
    portao >> publicado >> dbt_build(publicado) >> validate_gold(publicado) >> fim
    return portao, fim


# ---------------------------------------------------------------------- DAG
def criar_dag(cfg: dict, padrao: dict, anterior: dict | None) -> DAG:
    inicio: date = padrao["inicio"]
    dag = DAG(
        dag_id=cfg["dag_id"],
        description=cfg["titulo"],
        doc_md=f"### {cfg['titulo']}\nTodo dia às {_hora(cfg['horario'])} (Brasília). {cfg['descricao']}\n\n"
               f"Gerada por `dags/fabrica_dags` a partir de `dags/config/camadas.yaml`.",
        schedule=cfg["horario"],
        start_date=pendulum.datetime(inicio.year, inicio.month, inicio.day, tz=padrao["fuso"]),
        catchup=False,
        max_active_runs=padrao["max_execucoes_ativas"],
        max_active_tasks=cfg.get("max_tarefas_ativas", padrao["max_tarefas_ativas"]),
        default_args={
            "retries": padrao["tentativas"],
            "retry_delay": timedelta(minutes=padrao["espera_tentativa_min"]),
            "retry_exponential_backoff": float(padrao["multiplicador_espera"]),  # 5, 10, 20 min...
            "max_retry_delay": timedelta(minutes=padrao["espera_tentativa_max_min"]),
        },
        params=_params(cfg.get("params")),
        tags=[*padrao["tags"], cfg["camada"]],
    )
    with dag:
        espera = espera_camada_anterior(cfg, anterior) if anterior else None
        if cfg["camada"] == "gold":
            primeira, _ = etapas_gold(cfg)
            if espera:
                espera >> primeira
        else:
            for grupo in cfg["grupos"]:
                tg = grupo_da_fonte(cfg["camada"], grupo, cfg)
                if espera:
                    espera >> tg
    return dag


def criar_dags(config: dict | None = None) -> list[DAG]:
    """Uma DAG por camada, na ordem do YAML; cada uma espera a anterior."""
    config = config or carregar_config()
    camadas = config["camadas"]
    return [criar_dag(cfg, config["padrao"], camadas[i - 1] if i else None) for i, cfg in enumerate(camadas)]
