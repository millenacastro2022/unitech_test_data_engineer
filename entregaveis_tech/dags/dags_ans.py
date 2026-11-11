"""DAGs da ANS: Landing → Bronze → Silver → Gold, uma DAG por camada.

As DAGs não são escritas uma a uma: a fábrica (`fabrica_dags/fabrica.py`) lê
`config/camadas.yaml` e gera cada uma, com um TaskGroup por fonte e uma tarefa
mapeada por arquivo ou lote. Este arquivo só as registra para o Airflow.
"""

from fabrica_dags.fabrica import criar_dags

for _dag in criar_dags():
    globals()[_dag.dag_id] = _dag
