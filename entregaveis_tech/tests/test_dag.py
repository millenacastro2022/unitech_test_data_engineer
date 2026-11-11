"""DAGs geradas pela fábrica a partir de dags/config/camadas.yaml: importam sem erros, uma por
camada, com um TaskGroup por fonte, mapeamento dinâmico, pools e a espera da camada anterior."""

import json
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

airflow = pytest.importorskip("airflow")

try:  # Airflow 3.x
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # pragma: no cover
    from airflow.models.dagbag import DagBag

DAGS = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS))  # o Airflow já faz isso com a pasta de DAGs; aqui vale para o pytest

from fabrica_dags.fabrica import carregar_config  # noqa: E402

CONFIG = carregar_config()
CAMADAS = {c["camada"]: c for c in CONFIG["camadas"]}
DAG_IDS = [c["dag_id"] for c in CONFIG["camadas"]]
POOLS = json.loads((DAGS / "config" / "pools.json").read_text(encoding="utf-8"))
ESPERA = {"wait_upstream.check_run_type", "wait_upstream.manual_run", "wait_upstream.ready"}


@pytest.fixture(scope="module")
def bag():
    b = DagBag(dag_folder=str(DAGS))
    assert b.import_errors == {}
    return b


def test_uma_dag_por_camada_na_ordem_do_fluxo(bag):
    assert list(CAMADAS) == ["landing", "bronze", "silver", "gold"]
    assert set(bag.dag_ids) == set(DAG_IDS) == {"dag_landing_ans", "dag_bronze_ans", "dag_silver_ans", "dag_gold_ans"}


def test_grupos_sao_as_fontes_do_pacote():
    from ans_pipeline.entidades import ENTIDADES
    from ans_pipeline.fontes import FONTES

    assert CAMADAS["landing"]["grupos"] == list(FONTES)
    assert CAMADAS["bronze"]["grupos"] == list(FONTES)
    assert CAMADAS["silver"]["grupos"] == list(ENTIDADES)


@pytest.mark.parametrize("camada", ["landing", "bronze", "silver"])
def test_um_taskgroup_por_fonte_com_tarefa_mapeada(bag, camada):
    cfg = CAMADAS[camada]
    dag = bag.dags[cfg["dag_id"]]
    listar, processar = cfg["listar"]["tarefa"], cfg["processar"]["tarefa"]
    esperadas = {f"{g}.{t}" for g in cfg["grupos"] for t in (listar, processar)}
    assert set(dag.task_ids) == esperadas | (set() if camada == "landing" else ESPERA | {f"wait_upstream.wait_{_anterior(camada)}"})
    for g in cfg["grupos"]:
        t = dag.get_task(f"{g}.{processar}")
        assert hasattr(t, "partial_kwargs"), "mapeamento dinâmico (.expand)"
        assert t.upstream_task_ids == {f"{g}.{listar}"}, "cada grupo processa só o que listou"
        assert t.pool == cfg["processar"]["pool"]
        assert t.execution_timeout == timedelta(minutes=cfg["processar"]["limite_min"])
        assert t.map_index_template, "rótulo do arquivo/lote na UI"


def test_spark_e_downloads_limitados_por_pool():
    usados = {CAMADAS[c]["processar"]["pool"] for c in ("landing", "bronze", "silver")}
    usados.add(CAMADAS["gold"]["etapas"]["publish_silver"]["pool"])
    assert usados == set(POOLS) == {"spark", "ans_download"}
    assert CAMADAS["bronze"]["processar"]["pool"] == CAMADAS["silver"]["processar"]["pool"] == "spark"
    assert POOLS["spark"]["slots"] >= 1


def _anterior(camada: str) -> str:
    ordem = list(CAMADAS)
    return CAMADAS[ordem[ordem.index(camada) - 1]]["dag_id"]


@pytest.mark.parametrize("camada", ["bronze", "silver", "gold"])
def test_agendada_espera_a_camada_anterior_e_manual_segue(bag, camada):
    dag = bag.dags[CAMADAS[camada]["dag_id"]]
    sensor = dag.get_task(f"wait_upstream.wait_{_anterior(camada)}")
    assert type(sensor).__name__ == "ExternalTaskSensor"
    assert sensor.external_dag_id == _anterior(camada) and sensor.external_task_id is None
    assert sensor.execution_delta == timedelta(hours=2), "a execução do mesmo dia da camada anterior"
    assert set(sensor.allowed_states) == {"success", "failed"}, "espera terminar, qualquer que seja o resultado"
    assert sensor.mode == "reschedule" and sensor.soft_fail
    assert dag.get_task("wait_upstream.ready").trigger_rule == "none_failed"

    decidir = dag.get_task("wait_upstream.check_run_type").python_callable
    assert decidir(dag_run=SimpleNamespace(run_type="scheduled")) == sensor.task_id
    assert decidir(dag_run=SimpleNamespace(run_type="manual")) == "wait_upstream.manual_run"

    inicio = {t.task_id for t in dag.tasks if t.upstream_task_ids == {"wait_upstream.ready"}}
    raizes = {t.task_id for t in dag.tasks if not t.upstream_task_ids}
    assert raizes == {"wait_upstream.check_run_type"}, "nada começa antes da espera"
    assert inicio, "a espera libera a camada"


def test_horarios_em_sequencia_antes_do_expediente(bag):
    horas = []
    for dag_id in DAG_IDS:
        dag = bag.dags[dag_id]
        assert type(dag.timetable).__name__ == "CronTriggerTimetable"
        assert str(dag.timetable.timezone) == "America/Sao_Paulo"
        minuto, hora, *resto = dag.timetable.expression.split()
        assert resto == ["*", "*", "*"], "execução diária"
        horas.append(int(hora) * 60 + int(minuto))
        assert dag.catchup is False
        assert dag.max_active_runs == 1, "uma execução por vez: o processamento lê e grava o controle"
    assert horas == sorted(horas) and len(set(horas)) == 4, "landing < bronze < silver < gold"
    assert all(b - a >= 120 for a, b in zip(horas, horas[1:])), "2 horas entre uma camada e a seguinte"
    assert horas[-1] < 8 * 60, "Gold pronta antes do expediente"


def test_portao_da_gold_vem_antes_de_qualquer_carga(bag):
    gold = bag.dags["dag_gold_ans"]
    assert gold.get_task("validate_silver_complete").upstream_task_ids == {"wait_upstream.ready"}
    assert gold.get_task("publish_silver").upstream_task_ids == {"validate_silver_complete"}
    assert gold.get_task("publish_silver").pool == "spark"
    assert gold.get_task("dbt_build").upstream_task_ids == {"publish_silver"}, "o dbt lê a Silver publicada pelo Spark"
    assert gold.get_task("validate_gold").upstream_task_ids == {"publish_silver", "dbt_build"}
    assert gold.get_task("apply_retention").upstream_task_ids == {"validate_gold"}, "expurgo só depois da carga validada"
    assert gold.get_task("apply_retention").downstream_task_ids == set()


def test_retries_e_parametros(bag):
    for dag_id in DAG_IDS:
        for t in bag.dags[dag_id].tasks:
            assert t.retries >= 1, f"{dag_id}.{t.task_id}"
            assert t.retry_exponential_backoff == 2.0, "espera entre tentativas dobra (é multiplicador no Airflow 3)"
    assert set(bag.dags["dag_landing_ans"].params.keys()) == {"ufs"}, "sem carga histórica: a janela é fixa"
    for dag_id in ("dag_bronze_ans", "dag_silver_ans", "dag_gold_ans"):
        assert set(bag.dags[dag_id].params.keys()) == {"reprocessar"}


def test_fonte_nova_vira_um_grupo_novo_sem_codigo():
    """Escalar por configuração: uma fonte a mais no YAML gera mais um TaskGroup."""
    from fabrica_dags.fabrica import criar_dags

    config = carregar_config()
    config["camadas"][1]["grupos"] = [*config["camadas"][1]["grupos"], "nova_fonte"]
    bronze = criar_dags(config)[1]
    assert {"nova_fonte.list_pending", "nova_fonte.load_batch"} <= set(bronze.task_ids)
