"""Testa a leitura da Bronze e as regras da Silver (PySpark) com arquivos pequenos e sem S3.

Cada teste faz o caminho real: CSV da ANS -> tabela da Bronze (``bronze.ler_csv``) -> regra da Silver.
"""

import zipfile
from decimal import Decimal

import pytest

from ans_pipeline import bronze, silver
from ans_pipeline.entidades import ENTIDADES, SchemaDivergente
from ans_pipeline.periodo import agrupar_por_periodo

PARAMS = {"ingestion_date": "2026-11-07", "arquivo_origem": "s3://landing/teste.csv"}


def _silver(spark, entidade: str, caminho, params=PARAMS):
    """CSV -> Bronze -> Silver, como no pipeline."""
    return silver.transformar(spark, ENTIDADES[entidade], bronze.ler_csv(spark, caminho, ENTIDADES[entidade]), params)


def _linhas(df, colunas: str, ordem: str | None = None, filtro: str = "ds_motivo_rejeicao IS NULL"):
    """Linhas do resultado como tuplas, na ordem pedida (padrão: a primeira coluna)."""
    sel = df.filter(filtro).selectExpr(*[c.strip() for c in colunas.split(",")])
    ordem = [sel.columns[int(i) - 1] for i in (ordem or "1").split(",")]
    return [tuple(r) for r in sel.orderBy(*ordem).collect()]


# ---------------------------------------------------------------- utilidades
@pytest.mark.parametrize(
    "original, esperado",
    [("VL_SALDO_INICIAL", "vl_saldo_inicial"), ("Razão Social", "razao_social"), (" DATA ", "data")],
)
def test_normalizar_nome_coluna(original, esperado):
    assert bronze.normalizar_nome_coluna(original) == esperado


def test_detectar_encoding_latin1(escrever_csv):
    caminho = escrever_csv("latin.csv", ["A;B", "SAÚDE;ÇÃO"], encoding="latin-1")
    assert bronze.detectar_encoding(caminho) == "latin-1"
    caminho = escrever_csv("utf8.csv", ["A;B", "SAÚDE;ÇÃO"])
    assert bronze.detectar_encoding(caminho) == "utf-8"


def test_extrair_csv_de_zip(tmp_path, escrever_csv):
    csv = escrever_csv("1T2025.csv", ["A;B", "1;2"])
    zip_path = tmp_path / "1T2025.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv, arcname="pasta/1T2025.csv")
    destino = tmp_path / "out"
    destino.mkdir()
    extraido = bronze.extrair_csv(zip_path, destino)
    assert extraido == destino / "1T2025.csv"  # caminho interno do ZIP é descartado
    assert extraido.read_text().startswith("A;B")


def test_schema_divergente(spark, escrever_csv):
    caminho = escrever_csv("cadop.csv", ["REGISTRO_OPERADORA;CNPJ", '"123";"1"'])
    with pytest.raises(SchemaDivergente, match="colunas obrigatórias ausentes"):
        bronze.ler_csv(spark, caminho, ENTIDADES["operadoras"])


# ---------------------------------------------------------------- operadoras
CABECALHO_CADOP = (
    "REGISTRO_OPERADORA;CNPJ;RAZAO_SOCIAL;NOME_FANTASIA;MODALIDADE;LOGRADOURO;NUMERO;COMPLEMENTO;BAIRRO;"
    "CIDADE;UF;CEP;DDD;TELEFONE;FAX;ENDERECO_ELETRONICO;REPRESENTANTE;CARGO_REPRESENTANTE;"
    "REGIAO_DE_COMERCIALIZACAO;DATA_REGISTRO_ANS"
)


def test_operadoras(spark, escrever_csv):
    caminho = escrever_csv("cadop.csv", [
        CABECALHO_CADOP,
        # registro sem zeros à esquerda, e-mail inválido, nome fantasia vazio, UF inválida
        '"5711";"92.693.118/0001-60";"  BRADESCO   SAÚDE S.A. ";;"Seguradora Especializada em Saúde";"R";"1";;"B";"Barueri";"ZZ";"06454000";"11";"1234";;"email-invalido";"X";"Y";1;"2001-01-01"',
        # mesma operadora, registro mais recente => deve prevalecer
        '"005711";"92693118000160";"BRADESCO SAUDE S.A.";"BRADESCO";"Seguradora Especializada em Saúde";"R";"1";;"B";"Barueri";"SP";"6454000";"11";"1234";;"Contato@Bradesco.com.br";"X";"Y";1;"2002-02-02"',
        # sem razão social => quarentena
        '"999999";"1";;;"Medicina de Grupo";;;;;;"SP";;;;;;;;2;"2010-01-01"',
    ])
    df, metricas = _silver(spark, "operadoras", caminho)
    assert metricas == {"linhas_lidas": 3, "linhas_validas": 1, "linhas_rejeitadas": 1, "duplicatas_removidas": 1}

    [(registro, cnpj, razao, fantasia, modalidade, uf, cep, email, dt)] = _linhas(
        df, "cd_operadora, nr_cnpj, nm_razao_social, nm_fantasia, ds_modalidade, sg_uf, nr_cep, ds_email, dt_registro_ans"
    )
    assert (registro, cnpj, razao, fantasia) == ("005711", "92693118000160", "BRADESCO SAUDE S.A.", "BRADESCO")
    assert modalidade == "SEGURADORA ESPECIALIZADA EM SAÚDE"
    assert (uf, cep, email, str(dt)) == ("SP", "06454000", "contato@bradesco.com.br", "2002-02-02")
    assert _linhas(df, "ds_motivo_rejeicao", filtro="ds_motivo_rejeicao IS NOT NULL") == [("nm_razao_social ausente",)]


# ---------------------------------------------------------------- beneficiários
CABECALHO_SIB = (
    "ID_CMPT_MOVEL;CD_OPERADORA;NM_RAZAO_SOCIAL;NR_CNPJ;MODALIDADE_OPERADORA;SG_UF;CD_MUNICIPIO;NM_MUNICIPIO;"
    "TP_SEXO;DE_FAIXA_ETARIA;DE_FAIXA_ETARIA_REAJ;CD_PLANO;TP_VIGENCIA_PLANO;DE_CONTRATACAO_PLANO;"
    "DE_SEGMENTACAO_PLANO;DE_ABRG_GEOGRAFICA_PLANO;COBERTURA_ASSIST_PLAN;TIPO_VINCULO;QT_BENEFICIARIO_ATIVO;"
    "QT_BENEFICIARIO_ADERIDO;QT_BENEFICIARIO_CANCELADO;DT_CARGA"
)
LINHA_SIB = '"2025-06";"005711";"BRADESCO";"92693118000160";"SEGURADORA ESPECIALIZADA EM SAÚDE";"{uf}";"120040";"Rio Branco";"{sexo}";"{faixa}";"54 a 58 anos";"443100036";"P";"Coletivo Empresarial";"Amb + Hosp";"Nacional";"Médico-hospitalar";"Titular";{qt};0;0;"2025-07-20"'


def test_beneficiarios(spark, escrever_csv):
    caminho = escrever_csv("ben.csv", [
        CABECALHO_SIB,
        LINHA_SIB.format(uf="AC", sexo="M", faixa="55 a 59 anos", qt=8),
        LINHA_SIB.format(uf="AC", sexo="M", faixa="55 a 59 anos", qt=8),             # duplicata exata
        LINHA_SIB.format(uf="??", sexo="X", faixa="Informada Incorr", qt=""),         # inválidos tratados
        LINHA_SIB.format(uf="AC", sexo="F", faixa="55 a 59 anos", qt=-1),            # quantidade negativa
        LINHA_SIB.format(uf="AC", sexo="F", faixa="60 a 64 anos", qt=3).replace('"2025-06"', '"2025-05"', 1),
    ])
    df, metricas = _silver(spark, "beneficiarios", caminho, {**PARAMS, "competencia": "2025-06-01"})
    assert metricas == {"linhas_lidas": 5, "linhas_validas": 2, "linhas_rejeitadas": 2, "duplicatas_removidas": 1}

    validas = _linhas(df, "sg_uf, tp_sexo, ds_faixa_etaria, qt_beneficiario_ativo, cast(dt_competencia as string)")
    assert validas == [("AC", "M", "55 a 59 anos", 8, "2025-06-01"), ("XX", "I", "Não informada", 0, "2025-06-01")]
    rejeitadas = {m for (m,) in _linhas(df, "ds_motivo_rejeicao", filtro="ds_motivo_rejeicao IS NOT NULL")}
    assert rejeitadas == {"quantidade negativa", "competência diferente da esperada para o arquivo"}


# ---------------------------------------------------------------- financeiro
CABECALHO_DIOPS = '"DATA";"REG_ANS";"CD_CONTA_CONTABIL";"DESCRICAO";"VL_SALDO_INICIAL";"VL_SALDO_FINAL"'


def test_financeiro(spark, escrever_csv):
    caminho = escrever_csv("2T2025.csv", [
        CABECALHO_DIOPS,
        '"2025-04-01";"316903";"31";"RECEITAS";246948,98;395211,59',
        '"2025/04/01";"316903";"41";"EVENTOS";"1.000,00";"2.500,50"',   # formato antigo de data/valor
        '"01/04/2025";"5711";"31";"RECEITAS";0;10',                      # registro sem zeros à esquerda
        '"2025-04-01";"5711";"31";"RECEITAS";0;9',                       # mesma chave => mantém o maior saldo
        '"2025-01-01";"316903";"1";"ATIVO";1;2',                         # data de outro trimestre
        '"2025-04-01";"316903";"4";"DESPESAS";0;abc',                    # valor não numérico
    ])
    df, metricas = _silver(spark, "financeiro", caminho, {**PARAMS, "dt_trimestre": "2025-04-01"})
    assert metricas == {"linhas_lidas": 6, "linhas_validas": 3, "linhas_rejeitadas": 2, "duplicatas_removidas": 1}
    assert _linhas(df, "cd_operadora, cd_conta_contabil, vl_saldo_inicial, vl_saldo_final, vl_movimento_trimestre", "1, 2") == [
        ("005711", "31", Decimal("0.00"), Decimal("10.00"), Decimal("10.00")),
        ("316903", "31", Decimal("246948.98"), Decimal("395211.59"), Decimal("148262.61")),
        ("316903", "41", Decimal("1000.00"), Decimal("2500.50"), Decimal("1500.50")),
    ]


# ---------------------------------------------------------------- produtos
CABECALHO_PRODUTOS = (
    "ID_PLANO;CD_PLANO;NM_PLANO;REGISTRO_OPERADORA;RAZAO_SOCIAL;GR_MODALIDADE;PORTE_OPERADORA;VIGENCIA_PLANO;"
    "CONTRATACAO;GR_CONTRATACAO;SGMT_ASSISTENCIAL;GR_SGMT_ASSISTENCIAL;LG_ODONTOLOGICO;OBSTETRICIA;COBERTURA;"
    "TIPO_FINANCIAMENTO;ABRANGENCIA_COBERTURA;ID_GEO_COBERTURA;FATOR_MODERADOR;ACOMODACAO_HOSPITALAR;"
    "LIVRE_ESCOLHA;SITUACAO_PLANO;DT_SITUACAO;DT_REGISTRO_PLANO;DT_ATUALIZACAO"
)
LINHA_PRODUTO = '{id};"471016149";"PLANO X";"312592";"Unimed";"Cooperativa Médica";"Pequeno";"p";"Coletivo";"Coletivo";"Amb";"Amb";{odonto};;"Médico-hospitalar";;"Nacional";;;;;"{situacao}";"2023-08-09";"2014-04-03";"{atualizacao}"'


def test_produtos_prioriza_plano_ativo(spark, escrever_csv):
    caminho = escrever_csv("produtos.csv", [
        CABECALHO_PRODUTOS,
        LINHA_PRODUTO.format(id=1, odonto=0, situacao="Transferido", atualizacao="2026-11-07"),
        LINHA_PRODUTO.format(id=2, odonto=1, situacao="Ativo", atualizacao="2026-01-01"),
    ])
    df, metricas = _silver(spark, "produtos", caminho)
    assert metricas["linhas_validas"] == 1 and metricas["duplicatas_removidas"] == 1
    assert _linhas(df, "id_plano, ds_situacao_plano, fl_odontologico, tp_vigencia_plano, ds_modalidade") == [
        (2, "Ativo", True, "P", "COOPERATIVA MÉDICA")
    ]


# ---------------------------------------------------------------- lotes (um job Spark por lote)
def test_lotes_por_fonte_e_periodo():
    pendentes = [
        {"fonte": "beneficiarios", "particao": "competencia=202609/uf=SP", "chave": "a"},
        {"fonte": "beneficiarios", "particao": "competencia=202609/uf=AC", "chave": "b"},
        {"fonte": "beneficiarios", "particao": "competencia=202608/uf=AC", "chave": "c"},
        {"fonte": "financeiro", "particao": "ano=2026/trimestre=2", "chave": "d"},
        {"fonte": "operadoras", "particao": "snapshot", "chave": "e"},
    ]
    lotes = agrupar_por_periodo(pendentes)
    assert [[i["chave"] for i in lote] for lote in lotes] == [["c"], ["b", "a"], ["d"], ["e"]]


def test_particao_da_bronze_e_periodo_esperado_na_silver():
    from ans_pipeline.entidades import particao_tabela

    assert particao_tabela("beneficiarios", "competencia=202609/uf=SP", "2026-11-09") == "competencia=202609/uf=SP"
    assert particao_tabela("financeiro", "ano=2026/trimestre=2", "2026-11-09") == "ano=2026/trimestre=2"
    assert particao_tabela("produtos", "snapshot", "2026-11-09") == "data_snapshot=2026-11-09"
    assert silver.parametros_da_particao("competencia=202609/uf=SP") == {"competencia": "2026-09-01"}
    assert silver.parametros_da_particao("ano=2026/trimestre=2") == {"dt_trimestre": "2026-04-01"}
    assert silver.parametros_da_particao("data_snapshot=2026-11-09") == {}
