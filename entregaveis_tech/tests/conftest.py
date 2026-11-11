import sys
from pathlib import Path

import pytest

RAIZ = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(RAIZ / "src"))


@pytest.fixture(scope="session")
def spark():
    """Sessão Spark local mínima, compartilhada pelos testes (a JVM sobe uma vez só)."""
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    sessao = (SparkSession.builder.master("local[1]").appName("testes")
              .config("spark.ui.enabled", "false")
              .config("spark.sql.shuffle.partitions", "1")
              .config("spark.sql.session.timeZone", "UTC")
              .getOrCreate())
    sessao.sparkContext.setLogLevel("ERROR")
    yield sessao
    sessao.stop()


@pytest.fixture
def escrever_csv(tmp_path):
    """Grava um CSV no padrão da ANS (separador ';') e devolve o caminho."""

    def _escrever(nome: str, linhas: list[str], encoding: str = "utf-8") -> Path:
        caminho = tmp_path / nome
        caminho.write_bytes(("\r\n".join(linhas) + "\r\n").encode(encoding))
        return caminho

    return _escrever
