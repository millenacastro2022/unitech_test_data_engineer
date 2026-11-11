"""As quatro entidades do lakehouse e o que se espera de cada uma.

A Bronze usa ``colunas_obrigatorias`` para conferir o layout do arquivo da ANS; a Silver
usa a chave, a ordenação e o volume mínimo.
"""

from __future__ import annotations

from dataclasses import dataclass


class SchemaDivergente(Exception):
    """O arquivo da fonte não possui as colunas esperadas (mudança de layout na ANS)."""


@dataclass(frozen=True)
class Entidade:
    nome: str
    colunas_obrigatorias: tuple[str, ...]
    chave: tuple[str, ...] | None       # chave natural única na Silver (None = grão sem chave)
    ordenacao: tuple[str, ...]          # ordenação física do Parquet na Silver (compressão e pruning)
    min_linhas: int                     # volume mínimo esperado na Silver (detecta arquivo truncado)


ENTIDADES: dict[str, Entidade] = {
    "operadoras": Entidade(
        nome="operadoras",
        colunas_obrigatorias=("registro_operadora", "cnpj", "razao_social", "nome_fantasia", "modalidade",
                              "cidade", "uf", "regiao_de_comercializacao", "data_registro_ans"),
        chave=("cd_operadora",),
        ordenacao=("cd_operadora",),
        min_linhas=100,
    ),
    "produtos": Entidade(
        nome="produtos",
        colunas_obrigatorias=("id_plano", "cd_plano", "nm_plano", "registro_operadora", "gr_modalidade",
                              "vigencia_plano", "contratacao", "sgmt_assistencial", "cobertura",
                              "abrangencia_cobertura", "situacao_plano", "dt_atualizacao"),
        chave=("cd_operadora", "cd_plano"),
        ordenacao=("cd_operadora", "cd_plano"),
        min_linhas=1000,
    ),
    "beneficiarios": Entidade(
        nome="beneficiarios",
        colunas_obrigatorias=("id_cmpt_movel", "cd_operadora", "nm_razao_social", "modalidade_operadora",
                              "sg_uf", "cd_municipio", "tp_sexo", "de_faixa_etaria", "cd_plano",
                              "de_contratacao_plano", "cobertura_assist_plan", "qt_beneficiario_ativo"),
        chave=None,
        ordenacao=("cd_operadora", "cd_plano", "cd_municipio"),
        min_linhas=1,
    ),
    "financeiro": Entidade(
        nome="financeiro",
        colunas_obrigatorias=("data", "reg_ans", "cd_conta_contabil", "descricao",
                              "vl_saldo_inicial", "vl_saldo_final"),
        chave=("cd_operadora", "cd_conta_contabil"),
        ordenacao=("cd_operadora", "cd_conta_contabil"),
        min_linhas=1000,
    ),
}


def particao_tabela(fonte: str, particao_landing: str, ingestion_date: str) -> str:
    """Partição da Bronze e da Silver a partir da partição do arquivo na Landing.

    Beneficiários e financeiro mantêm o período (``competencia=AAAAMM/uf=XX``,
    ``ano=AAAA/trimestre=T``); os cadastros viram ``data_snapshot=AAAA-MM-DD``.
    """
    if fonte in ("operadoras", "produtos"):
        return f"data_snapshot={ingestion_date}"
    return particao_landing


def tabela_silver(entidade: str) -> str:
    """Nome da tabela da entidade na Silver, igual no Parquet (s3://silver/tb_<entidade>/) e no
    schema silver do PostgreSQL (silver.tb_<entidade>): prefixo tb_ do padrão de nomenclatura."""
    return f"tb_{entidade}"
