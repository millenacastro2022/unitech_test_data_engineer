"""Regras da camada Silver, uma função por entidade (PySpark).

Entrada: o CSV original como DataFrame, todas as colunas como texto e com nomes em
snake_case (nomes da Bronze). Saída: as colunas tipadas, padronizadas e renomeadas com o prefixo
de duas letras do padrão de nomenclatura (cd_, nr_, nm_, ds_, sg_, tp_, dt_, dh_, qt_, vl_, fl_, id_),
já sem duplicatas, mais ``ds_motivo_rejeicao``: preenchida, a linha vai para a quarentena em vez da Silver.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from ans_pipeline.limpeza import (
    categoria, codigo, data_br, decimal_br, digitos, email_valido, inteiro, txt, txt_upper, uf_valida,
)

MOTIVO = "ds_motivo_rejeicao"
CONTROLE = ("dh_ingestao", "ds_arquivo_origem", "dh_processamento")


def _metadados(df: DataFrame, p: dict) -> list[Column]:
    """Controle da linha: quando o arquivo chegou à Landing (vem da Bronze), de onde veio e quando
    a Silver a processou (UTC)."""
    ingestao = F.col("dh_ingestao") if "dh_ingestao" in df.columns else F.lit(p["ingestion_date"]).cast("timestamp_ntz")
    return [
        ingestao.cast("timestamp_ntz").alias("dh_ingestao"),
        F.lit(p["arquivo_origem"]).alias("ds_arquivo_origem"),
        F.current_timestamp().cast("timestamp_ntz").alias("dh_processamento"),
    ]


def _motivo(*regras: tuple[Column, str]) -> Column:
    """A primeira regra violada vira o motivo da rejeição; nenhuma, NULL."""
    expr = None
    for condicao, texto in regras:
        expr = F.when(condicao, texto) if expr is None else expr.when(condicao, texto)
    return expr.alias(MOTIVO)


def _um_por_chave(df: DataFrame, chave: list[str], ordem: list[Column]) -> DataFrame:
    """Remove duplicatas da chave natural mantendo a primeira linha na ``ordem``.
    Linhas rejeitadas não disputam com as válidas e seguem todas para a quarentena."""
    w = Window.partitionBy(F.col(MOTIVO).isNull(), *chave).orderBy(*ordem)
    return (df.withColumn("_n", F.row_number().over(w))
              .filter(F.col(MOTIVO).isNotNull() | (F.col("_n") == 1))
              .drop("_n"))


# ---------------------------------------------------------------------- operadoras (CADOP)
def operadoras(df: DataFrame, p: dict) -> DataFrame:
    """Uma linha por operadora (cd_operadora = registro ANS)."""
    tipado = df.select(
        codigo("registro_operadora", 6).alias("cd_operadora"),
        codigo("cnpj", 14).alias("nr_cnpj"),
        txt("razao_social").alias("nm_razao_social"),
        txt("nome_fantasia").alias("nm_fantasia"),
        categoria(txt_upper("modalidade"), "NÃO INFORMADA").alias("ds_modalidade"),
        txt("logradouro").alias("ds_logradouro"),
        txt("numero").alias("nr_endereco"),
        txt("complemento").alias("ds_complemento"),
        txt("bairro").alias("nm_bairro"),
        txt("cidade").alias("nm_cidade"),
        uf_valida("uf").alias("sg_uf"),
        codigo("cep", 8).alias("nr_cep"),
        digitos("ddd").alias("nr_ddd"),
        digitos("telefone").alias("nr_telefone"),
        digitos("fax").alias("nr_fax"),
        email_valido("endereco_eletronico").alias("ds_email"),
        txt("representante").alias("nm_representante"),
        txt("cargo_representante").alias("ds_cargo_representante"),
        inteiro("regiao_de_comercializacao", "smallint").alias("cd_regiao_comercializacao"),
        data_br("data_registro_ans").alias("dt_registro_ans"),
        *_metadados(df, p),
    )
    validado = tipado.withColumn(MOTIVO, _motivo(
        (F.col("cd_operadora").isNull(), "cd_operadora ausente ou inválido"),
        (F.col("nm_razao_social").isNull(), "nm_razao_social ausente"),
    ))
    # Duplicatas: fica o registro mais recente da operadora.
    return _um_por_chave(validado, ["cd_operadora"],
                         [F.col("dt_registro_ans").desc_nulls_last(), F.col("nr_cnpj").asc_nulls_last()])


# ---------------------------------------------------------------------- produtos (pda-008)
def produtos(df: DataFrame, p: dict) -> DataFrame:
    """Chave natural (cd_operadora, cd_plano): é a chave usada pela base de beneficiários."""
    lg_odonto = inteiro("lg_odontologico")
    tipado = df.select(
        inteiro("id_plano").alias("id_plano"),
        txt("cd_plano").alias("cd_plano"),
        txt("nm_plano").alias("nm_plano"),
        codigo("registro_operadora", 6).alias("cd_operadora"),
        txt("razao_social").alias("nm_razao_social"),
        categoria(txt_upper("gr_modalidade"), "NÃO INFORMADA").alias("ds_modalidade"),
        txt("porte_operadora").alias("ds_porte_operadora"),
        txt_upper("vigencia_plano").alias("tp_vigencia_plano"),  # A = antes da Lei 9.656/98, P = posterior
        categoria("contratacao", "Não Informado").alias("ds_contratacao"),
        categoria("gr_contratacao", "Não Informado").alias("ds_grupo_contratacao"),
        categoria("sgmt_assistencial", "Não Informado").alias("ds_segmentacao_assistencial"),
        categoria("gr_sgmt_assistencial", "Não Informado").alias("ds_grupo_segmentacao_assistencial"),
        F.when(lg_odonto == 1, True).when(lg_odonto == 0, False).alias("fl_odontologico"),
        txt("obstetricia").alias("ds_obstetricia"),
        categoria("cobertura", "Não Informado").alias("ds_cobertura"),
        txt("tipo_financiamento").alias("tp_financiamento"),
        categoria("abrangencia_cobertura", "Não Informado").alias("ds_abrangencia_cobertura"),
        txt("id_geo_cobertura").alias("id_geo_cobertura"),
        txt("fator_moderador").alias("ds_fator_moderador"),
        txt("acomodacao_hospitalar").alias("ds_acomodacao_hospitalar"),
        txt("livre_escolha").alias("ds_livre_escolha"),
        categoria("situacao_plano", "Não Informado").alias("ds_situacao_plano"),
        data_br("dt_situacao").alias("dt_situacao"),
        data_br("dt_registro_plano").alias("dt_registro_plano"),
        data_br("dt_atualizacao").alias("dt_atualizacao"),
        *_metadados(df, p),
    )
    validado = tipado.withColumn(MOTIVO, _motivo(
        (F.col("cd_operadora").isNull(), "cd_operadora ausente ou inválido"),
        (F.col("cd_plano").isNull(), "cd_plano ausente"),
    ))
    # Duplicatas: a fonte traz o mesmo (operadora, plano) mais de uma vez (ex.: plano
    # transferido). Prioriza o plano ativo e a atualização mais recente.
    return _um_por_chave(validado, ["cd_operadora", "cd_plano"], [
        (F.col("ds_situacao_plano") == "Ativo").desc(), F.col("dt_atualizacao").desc_nulls_last(),
        F.col("dt_situacao").desc_nulls_last(), F.col("id_plano").desc_nulls_last(),
    ])


# ---------------------------------------------------------------------- beneficiários (pda-024)
def beneficiarios(df: DataFrame, p: dict) -> DataFrame:
    """Grão mantido da fonte: competência x operadora x município x sexo x faixa etária x
    plano x tipo de vínculo. Parâmetro ``competencia``: 1º dia do mês esperado no arquivo."""
    sexo = txt_upper("tp_sexo")
    qt = {c: F.coalesce(inteiro(c), F.lit(0).cast("bigint"))
          for c in ("qt_beneficiario_ativo", "qt_beneficiario_aderido", "qt_beneficiario_cancelado")}
    tipado = df.select(
        data_br(F.concat(txt("id_cmpt_movel"), F.lit("-01"))).alias("dt_competencia"),
        codigo("cd_operadora", 6).alias("cd_operadora"),
        txt("nm_razao_social").alias("nm_razao_social"),
        codigo("nr_cnpj", 14).alias("nr_cnpj"),
        categoria(txt_upper("modalidade_operadora"), "NÃO INFORMADA").alias("ds_modalidade"),
        F.coalesce(uf_valida("sg_uf"), F.lit("XX")).alias("sg_uf"),  # XX = UF não identificada
        digitos("cd_municipio").alias("cd_municipio"),
        txt("nm_municipio").alias("nm_municipio"),
        F.when(sexo == "M", "M").when(sexo == "F", "F").otherwise("I").alias("tp_sexo"),
        F.when(txt("de_faixa_etaria").isNull() | F.col("de_faixa_etaria").ilike("Informada Incorr%"), "Não informada")
         .otherwise(txt("de_faixa_etaria")).alias("ds_faixa_etaria"),
        categoria("de_faixa_etaria_reaj", "Não informada").alias("ds_faixa_etaria_reajuste"),
        F.coalesce(txt("cd_plano"), F.lit("NI")).alias("cd_plano"),
        txt_upper("tp_vigencia_plano").alias("tp_vigencia_plano"),
        categoria("de_contratacao_plano", "Não Identificado").alias("ds_contratacao"),
        categoria("de_segmentacao_plano", "Não Identificado").alias("ds_segmentacao"),
        categoria("de_abrg_geografica_plano", "Não Identificado").alias("ds_abrangencia_geografica"),
        categoria("cobertura_assist_plan", "Não Identificado").alias("ds_cobertura_assistencial"),
        categoria("tipo_vinculo", "Não Identificado").alias("tp_vinculo"),
        *(v.alias(c) for c, v in qt.items()),
        data_br("dt_carga").alias("dt_carga"),
        *_metadados(df, p),
    )
    negativa = reduce_ou([F.col(c) < 0 for c in qt])
    validado = tipado.withColumn(MOTIVO, _motivo(
        (F.col("cd_operadora").isNull(), "cd_operadora ausente ou inválido"),
        (F.col("dt_competencia").isNull(), "competência ausente ou inválida"),
        (F.col("dt_competencia") != F.lit(p["competencia"]).cast("date"), "competência diferente da esperada para o arquivo"),
        (negativa, "quantidade negativa"),
    ))
    # Duplicatas: linhas idênticas (mesmo grão e mesmas quantidades) seriam contadas em dobro.
    return validado.distinct()


# ---------------------------------------------------------------------- financeiro (DIOPS)
def financeiro(df: DataFrame, p: dict) -> DataFrame:
    """Um arquivo por trimestre. Parâmetro ``dt_trimestre``: 1º dia do trimestre (do nome do arquivo).

    Contas de resultado (classes 3 e 4) são ACUMULADAS no ano: VL_SALDO_FINAL é o acumulado
    até o fim do trimestre e VL_SALDO_INICIAL até o fim do anterior; o valor do próprio
    trimestre é final - inicial."""
    zero = F.lit(0).cast("decimal(20,2)")
    tipado = df.select(
        F.lit(p["dt_trimestre"]).cast("date").alias("dt_trimestre"),
        data_br("data").alias("dt_referencia_arquivo"),
        codigo("reg_ans", 6).alias("cd_operadora"),
        digitos("cd_conta_contabil").alias("cd_conta_contabil"),
        txt("descricao").alias("ds_conta_contabil"),
        F.coalesce(decimal_br("vl_saldo_inicial"), zero).alias("vl_saldo_inicial"),
        decimal_br("vl_saldo_final").alias("vl_saldo_final"),
        *_metadados(df, p),
    )
    validado = tipado.select(
        "dt_trimestre", "dt_referencia_arquivo", "cd_operadora", "cd_conta_contabil", "ds_conta_contabil",
        "vl_saldo_inicial", "vl_saldo_final",
        (F.col("vl_saldo_final") - F.col("vl_saldo_inicial")).cast("decimal(20,2)").alias("vl_movimento_trimestre"),
        *CONTROLE,
        _motivo(
            (F.col("cd_operadora").isNull(), "cd_operadora ausente ou inválido"),
            (F.col("cd_conta_contabil").isNull(), "conta contábil ausente"),
            (F.col("vl_saldo_final").isNull(), "saldo final ausente ou não numérico"),
            (F.col("dt_referencia_arquivo").isNotNull() & (F.col("dt_referencia_arquivo") != F.col("dt_trimestre")),
             "data diferente do trimestre do arquivo"),
        ),
    )
    # Duplicatas: uma linha por (operadora, conta) no trimestre. Em reenvio com valores
    # diferentes, fica o maior saldo final em módulo.
    return _um_por_chave(validado, ["cd_operadora", "cd_conta_contabil"],
                         [F.abs("vl_saldo_final").desc(), F.col("vl_saldo_inicial").desc()])


def reduce_ou(condicoes: list[Column]) -> Column:
    expr = condicoes[0]
    for c in condicoes[1:]:
        expr = expr | c
    return expr


REGRAS = {"operadoras": operadoras, "produtos": produtos, "beneficiarios": beneficiarios, "financeiro": financeiro}
