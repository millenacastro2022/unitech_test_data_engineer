"""Portão da Gold: a decisão de seguir ou parar, sem banco nem storage."""

from ans_pipeline.quality import avaliar_silver_completa

URI = "s3://silver/beneficiarios/competencia=202512/uf={uf}/beneficiarios.parquet"


def _registradas(*ufs):
    return [{"entidade": "beneficiarios", "particao": f"competencia=202512/uf={uf}", "uri": URI.format(uf=uf)} for uf in ufs]


def test_silver_completa_libera_a_gold():
    registradas = _registradas("AC", "RR") + [
        {"entidade": "operadoras", "particao": "data_snapshot=2026-11-08",
         "uri": "s3://silver/operadoras/data_snapshot=2026-11-08/operadoras.parquet"},
    ]
    falhas, resumo = avaliar_silver_completa([], registradas, {r["uri"] for r in registradas})
    assert falhas == []
    assert resumo["particoes_registradas"] == 3
    assert resumo["particoes_por_entidade"] == {"beneficiarios": 2, "operadoras": 1}


def test_arquivo_da_bronze_nao_processado_bloqueia():
    pendentes = [{"fonte": "beneficiarios", "particao": "competencia=202511/uf=AC"}]
    registradas = _registradas("AC")
    falhas, resumo = avaliar_silver_completa(pendentes, registradas, {r["uri"] for r in registradas})
    assert len(falhas) == 1 and "ainda não processado" in falhas[0] and "competencia=202511/uf=AC" in falhas[0]
    assert resumo["pendentes"] == ["beneficiarios competencia=202511/uf=AC"]


def test_particao_registrada_sem_arquivo_no_storage_bloqueia():
    registradas = _registradas("AC", "SP")
    falhas, resumo = avaliar_silver_completa([], registradas, {URI.format(uf="AC")})
    assert len(falhas) == 1 and "sem arquivo no storage" in falhas[0]
    assert resumo["ausentes_no_storage"] == ["beneficiarios competencia=202512/uf=SP"]


def test_lista_longa_e_resumida_na_mensagem():
    pendentes = [{"fonte": "beneficiarios", "particao": f"competencia=202512/uf={i:02d}"} for i in range(28)]
    falhas, resumo = avaliar_silver_completa(pendentes, [], set(), limite_lista=5)
    assert "28 item(ns)" in falhas[0] and "e mais 23" in falhas[0]
    assert len(resumo["pendentes"]) == 28
