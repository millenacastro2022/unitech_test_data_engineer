"""Retenção: quais períodos ficam e o que a Bronze deixa de baixar, sem banco nem storage."""

import pytest

from ans_pipeline import retencao


def test_periodo_de_particoes_e_chaves():
    assert retencao.periodo("beneficiarios", "competencia=202609/uf=SP") == "202609"
    assert retencao.periodo("beneficiarios",
                            "beneficiarios/ingestion_date=2026-11-07/competencia=202607/pda-024-icb-AC-2026_07.zip") == "202607"
    assert retencao.periodo("financeiro", "ano=2026/trimestre=2") == "2026-T2"
    assert retencao.periodo("financeiro", "_quarentena/financeiro/ano=2025/trimestre=4/rejeitados.parquet") == "2025-T4"
    assert retencao.periodo("operadoras", "snapshot") is None
    assert retencao.periodo("produtos", "data_snapshot=2026-11-09") is None


def test_janela_fica_com_os_mais_recentes():
    retidos = retencao.janela(
        {"beneficiarios": ["202512", "202607", "202609", "202608", "202606"],
         "financeiro": ["2025-T4", "2026-T2", "2026-T1"]},
        meses=3, trimestres=1)
    assert retidos == {"beneficiarios": {"202607", "202608", "202609"}, "financeiro": {"2026-T2"}}
    # a virada de ano não confunde a ordem
    assert retencao.janela({"beneficiarios": ["202612", "202701", "202611", "202610"]}, 3, 1)["beneficiarios"] == {
        "202611", "202612", "202701"}
    with pytest.raises(ValueError):
        retencao.janela({}, meses=0)


def test_dentro_da_janela():
    retidos = {"beneficiarios": {"202607", "202608", "202609"}, "financeiro": {"2026-T2"}}
    assert retencao.dentro("beneficiarios", "competencia=202609/uf=XX", retidos)
    assert not retencao.dentro("beneficiarios", "competencia=202606/uf=SP", retidos)
    assert not retencao.dentro("financeiro", "ano=2026/trimestre=1", retidos)
    assert retencao.dentro("operadoras", "snapshot", retidos), "cadastros ficam fora da regra"


def _nova(fonte, particao):
    return {"fonte": fonte, "particao": particao, "rotulo": f"{fonte} {particao}"}


def test_mes_novo_entra_e_empurra_o_mais_antigo():
    na_bronze = [("beneficiarios", f"competencia={c}/uf=SP") for c in ("202607", "202608", "202609")]
    novidades = [_nova("beneficiarios", "competencia=202610"), _nova("beneficiarios", "competencia=202607")]
    manter, fora, retidos = retencao.filtrar_novidades(novidades, na_bronze, meses=3, trimestres=1)
    assert retidos["beneficiarios"] == {"202608", "202609", "202610"}
    assert [n["particao"] for n in manter] == ["competencia=202610"]
    assert [n["particao"] for n in fora] == ["competencia=202607"], "republicação de mês que saiu não é baixada"


def test_mes_expurgado_nao_volta_a_ser_baixado():
    # a Bronze consulta 6 meses; os 3 mais antigos já foram expurgados e aparecem como "novos"
    na_bronze = [("beneficiarios", f"competencia={c}/uf=AC") for c in ("202607", "202608", "202609")]
    novidades = [_nova("beneficiarios", f"competencia={c}") for c in ("202604", "202605", "202606")]
    novidades += [_nova("financeiro", "ano=2026/trimestre=1"), _nova("financeiro", "ano=2026/trimestre=2")]
    manter, fora, _ = retencao.filtrar_novidades(novidades, na_bronze + [("financeiro", "ano=2026/trimestre=2")],
                                                 meses=3, trimestres=1)
    assert [n["particao"] for n in manter] == ["ano=2026/trimestre=2"]
    assert len(fora) == 4


def test_primeira_carga_fica_com_os_publicados_mais_recentes():
    # Bronze vazia: dos meses publicados na janela consultada, ficam os 3 mais recentes
    novidades = [_nova("beneficiarios", f"competencia={c}") for c in ("202605", "202606", "202607", "202608", "202609")]
    novidades += [_nova("operadoras", "")]
    manter, fora, _ = retencao.filtrar_novidades(novidades, [], meses=3, trimestres=1)
    assert [n["particao"] for n in manter] == ["competencia=202607", "competencia=202608", "competencia=202609", ""]
    assert [n["particao"] for n in fora] == ["competencia=202605", "competencia=202606"]


def test_chaves_da_gold():
    assert retencao.sk_competencia("202609") == 20260901
    assert retencao.sk_trimestre("2026-T2") == 20260401
    assert retencao.sk_trimestre("2025-T4") == 20251001
    assert retencao.unidade_gold("beneficiarios", "202609") == "ft_beneficiarios:2026-09"
    assert retencao.unidade_gold("financeiro", "2026-T2") == "ft_financeiro:2026-T2"
