from datetime import date, datetime

import pytest

from ans_pipeline import fontes
from ans_pipeline.landing import chave_landing
from ans_pipeline.periodo import Competencia

BASE = "https://dadosabertos.ans.gov.br/FTP/PDA"


@pytest.mark.parametrize("valor", ["2025-06", "202506", " 2025-06 "])
def test_parse_competencia(valor):
    c = Competencia.parse(valor)
    assert (c.ano, c.mes) == (2025, 6)
    assert c.iso == "2025-06" and c.yyyymm == "202506"


@pytest.mark.parametrize("valor", ["2025-13", "2025-6", "25-06", "abc", ""])
def test_parse_competencia_invalida(valor):
    with pytest.raises(ValueError):
        Competencia.parse(valor)


@pytest.mark.parametrize(
    "mes, trimestre, fim, sk_trimestre",
    [(1, 1, False, 20250101), (3, 1, True, 20250101), (6, 2, True, 20250401), (11, 4, False, 20251001)],
)
def test_trimestre(mes, trimestre, fim, sk_trimestre):
    c = Competencia(2025, mes)
    assert c.trimestre == trimestre
    assert c.fim_de_trimestre is fim
    assert c.sk_trimestre == sk_trimestre


def test_competencia_de_data_e_chaves():
    c = Competencia.from_date(datetime(2025, 12, 1, 0, 0))
    assert c.primeiro_dia == date(2025, 12, 1)
    assert c.sk_data == 20251201


def test_urls_das_fontes():
    c = Competencia(2025, 6)
    assert fontes.beneficiarios(BASE, c, "sp").url == (
        f"{BASE}/informacoes_consolidadas_de_beneficiarios-024/202506/pda-024-icb-SP-2025_06.zip"
    )
    assert fontes.financeiro(BASE, 2025, 2).url == f"{BASE}/demonstracoes_contabeis/2025/2T2025.zip"
    assert fontes.operadoras(BASE).nome_arquivo == "Relatorio_cadop.csv"
    assert fontes.produtos(BASE).nome_arquivo.startswith("pda-008-caracteristicas")


def test_janela_de_competencias_e_trimestres():
    from ans_pipeline.periodo import janela_competencias, trimestres_fechados

    # 8/nov/2026: os três meses fechados anteriores e os quatro trimestres já encerrados
    assert [c.iso for c in janela_competencias(date(2026, 11, 8), 3)] == ["2026-08", "2026-09", "2026-10"]
    assert [c.iso for c in janela_competencias(date(2026, 1, 15), 2)] == ["2025-11", "2025-12"]
    assert trimestres_fechados(date(2026, 11, 8), 4) == [(2025, 4), (2026, 1), (2026, 2), (2026, 3)]
    assert trimestres_fechados(date(2026, 1, 1), 1) == [(2025, 4)]
    assert trimestres_fechados(date(2026, 3, 31), 1) == [(2025, 4)], "1º tri ainda não fechou em março"


def test_particao_de_controle_e_candidatos():
    sp = fontes.beneficiarios(BASE, Competencia(2025, 12), "sp")
    assert sp.particao_controle == "competencia=202512/uf=SP"
    assert fontes.financeiro(BASE, 2025, 4).particao_controle == "ano=2025/trimestre=4"
    assert fontes.operadoras(BASE).particao_controle == "snapshot"
    assert fontes.ArquivoFonte.from_dict(sp.to_dict()) == sp

    lista = fontes.candidatos(BASE, [Competencia(2025, 11), Competencia(2025, 12)], [(2025, 4)], ["AC", "SP"],
                              ["operadoras", "beneficiarios", "financeiro"])
    assert [a.fonte for a in lista].count("beneficiarios") == 4
    assert {a.fonte for a in lista} == {"operadoras", "beneficiarios", "financeiro"}


def test_mesma_publicacao():
    from ans_pipeline.landing import mesma_publicacao

    anterior = {"origem_etag": '"abc"', "origem_last_modified": "Thu, 05 Nov 2026 15:08:44 GMT", "bytes": 10}
    assert mesma_publicacao({"etag": '"abc"', "last_modified": "outra", "bytes": 99}, anterior)
    assert not mesma_publicacao({"etag": '"xyz"', "last_modified": anterior["origem_last_modified"], "bytes": 10}, anterior)
    sem_etag = {**anterior, "origem_etag": None}
    assert mesma_publicacao({"etag": None, "last_modified": anterior["origem_last_modified"], "bytes": 10}, sem_etag)
    assert not mesma_publicacao({"etag": None, "last_modified": "nova data", "bytes": 10}, sem_etag)
    assert not mesma_publicacao({"etag": '"abc"', "last_modified": None, "bytes": 1}, None)


def test_chave_landing_por_data_de_ingestao():
    arquivo = fontes.beneficiarios(BASE, Competencia(2025, 6), "AC")
    assert chave_landing(arquivo, date(2026, 11, 7)) == (
        "beneficiarios/ingestion_date=2026-11-07/competencia=202506/pda-024-icb-AC-2025_06.zip"
    )
    assert chave_landing(fontes.operadoras(BASE), date(2026, 11, 7)) == (
        "operadoras/ingestion_date=2026-11-07/Relatorio_cadop.csv"
    )
