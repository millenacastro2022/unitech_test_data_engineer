"""Funções de limpeza da Silver, como expressões do PySpark (``Column``).

Cada função recebe o nome de uma coluna (ou uma expressão) e devolve uma expressão nova;
nada é avaliado linha a linha em Python, então o Spark executa tudo na JVM.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

UFS_VALIDAS = (
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
)


def _c(x: str | Column) -> Column:
    return F.col(x) if isinstance(x, str) else x


def txt(x: str | Column) -> Column:
    """Texto sem espaços duplicados nem nas bordas; vazio vira NULL."""
    return F.nullif(F.trim(F.regexp_replace(_c(x), r"\s+", " ")), F.lit(""))


def txt_upper(x: str | Column) -> Column:
    """Texto categórico padronizado em maiúsculas (ex.: modalidade da operadora)."""
    return F.upper(txt(x))


def categoria(x: str | Column, padrao: str) -> Column:
    """Categoria com valor padrão explícito quando ausente (evita NULL em agrupamentos de BI)."""
    return F.coalesce(txt(x), F.lit(padrao))


def digitos(x: str | Column) -> Column:
    """Só os dígitos (CNPJ, CEP, telefone...)."""
    return F.nullif(F.regexp_replace(_c(x), r"[^0-9]", ""), F.lit(""))


def codigo(x: str | Column, largura: int) -> Column:
    """Código numérico de largura fixa com zeros à esquerda (registro ANS = 6, CNPJ = 14,
    CEP = 8). Mais dígitos que a largura é inválido e vira NULL."""
    d = digitos(x)
    return F.when(F.length(d) <= largura, F.lpad(d, largura, "0"))


def data_br(x: str | Column) -> Column:
    """Datas: a ANS usa formatos diferentes conforme o ano do arquivo."""
    t = txt(x)
    return F.coalesce(*(F.try_to_timestamp(t, F.lit(f)) for f in ("yyyy-MM-dd", "dd/MM/yyyy", "yyyy/MM/dd"))).cast("date")


def decimal_br(x: str | Column) -> Column:
    """Valores no padrão brasileiro ("1.234,56" ou "1234,56"); também aceita "1234.56"."""
    t = txt(x)
    br = F.replace(F.replace(t, F.lit("."), F.lit("")), F.lit(","), F.lit("."))
    return F.when(t.contains(","), br).otherwise(t).try_cast("decimal(20,2)")


def inteiro(x: str | Column, tipo: str = "bigint") -> Column:
    return txt(x).try_cast(tipo)


def uf_valida(x: str | Column) -> Column:
    """UF entre as 27; qualquer outro valor vira NULL (o chamador decide o que fazer)."""
    u = F.upper(txt(x))
    return F.when(u.isin(*UFS_VALIDAS), u)


def email_valido(x: str | Column) -> Column:
    """E-mail sintaticamente válido, em minúsculas; inválido vira NULL."""
    e = F.lower(txt(x))
    return F.when(e.rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), e)
