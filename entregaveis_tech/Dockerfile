FROM apache/airflow:3.3.2-python3.12

# Java 17 para o PySpark (Spark 4 roda em modo local dentro da task do Airflow) e o
# driver JDBC do PostgreSQL, usado pelo Spark para gravar a área de staging da Gold.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends default-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /opt/spark/jars \
 && curl -fsSL -o /opt/spark/jars/postgresql.jar \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
ENV JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_JDBC_JAR=/opt/spark/jars/postgresql.jar

USER airflow
COPY requirements.txt /requirements.txt
# Fixar apache-airflow na mesma instalação evita que alguma dependência atualize o Airflow.
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# dbt num virtualenv próprio: as dependências do dbt-core não se misturam com as do Airflow.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-core==1.12.6" "dbt-postgres==1.11.0" \
 && /home/airflow/dbt-venv/bin/dbt --version
ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt
