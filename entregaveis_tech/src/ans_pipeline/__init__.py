"""Pipeline de dados da ANS em Arquitetura Medalhão (Bronze -> Silver -> Gold).

O pacote não depende do Airflow: cada módulo pode ser executado e testado
isoladamente. As DAGs em ``dags/`` (dag_bronze_ans, dag_silver_ans e dag_gold_ans)
apenas orquestram estas funções.
"""
