"""Verificações de qualidade de dados das camadas Silver e Gold.

Falhas levantam ``FalhaQualidade`` e interrompem a task (dado reprovado não avança de camada).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ans_pipeline.config import Settings, get_settings
from ans_pipeline.periodo import Competencia

if TYPE_CHECKING:  # evita import circular em tempo de execução
    from pyspark.sql import DataFrame

    from ans_pipeline.entidades import Entidade

log = logging.getLogger(__name__)

# Acima disso o arquivo provavelmente mudou de layout/semântica: melhor parar e investigar.
TAXA_MAXIMA_REJEICAO = 0.05


class FalhaQualidade(Exception):
    pass


def validar_silver(tratado: "DataFrame", entidade: "Entidade", metricas: dict[str, int]) -> None:
    """Executada sobre o resultado da regra (Spark) antes da escrita no Object Storage."""
    falhas: list[str] = []
    lidas, validas, rejeitadas = metricas["linhas_lidas"], metricas["linhas_validas"], metricas["linhas_rejeitadas"]

    if validas < entidade.min_linhas:
        falhas.append(f"volume: {validas} linhas válidas (mínimo esperado {entidade.min_linhas})")
    if lidas and rejeitadas / lidas > TAXA_MAXIMA_REJEICAO:
        falhas.append(
            f"rejeição: {rejeitadas}/{lidas} linhas ({rejeitadas / lidas:.1%}) acima de {TAXA_MAXIMA_REJEICAO:.0%}"
        )

    if entidade.chave:
        from pyspark.sql import functions as F

        colunas = ", ".join(entidade.chave)
        validas = tratado.filter(F.col("ds_motivo_rejeicao").isNull())
        nulos = validas.filter(" OR ".join(f"{c} IS NULL" for c in entidade.chave)).count()
        if nulos:
            falhas.append(f"completude: {nulos} linhas com chave ({colunas}) nula")
        duplicadas = validas.groupBy(*entidade.chave).count().filter("count > 1").count()
        if duplicadas:
            falhas.append(f"unicidade: {duplicadas} chaves ({colunas}) duplicadas")

    if falhas:
        raise FalhaQualidade(f"Silver/{entidade.nome} reprovada: " + "; ".join(falhas))
    log.info("Silver/%s aprovada: %s", entidade.nome, metricas)


# ---------------------------------------------------------------------- portão da Gold
def avaliar_silver_completa(
    pendentes: list[dict], registradas: list[dict], uris_existentes: set[str], limite_lista: int = 10
) -> tuple[list[str], dict]:
    """Decide se a Silver está completa para seguir para a Gold (sem acesso a banco ou storage).

    ``pendentes``: o que ainda falta passar pelas camadas anteriores (Landing -> Bronze -> Silver).
    ``registradas``: partições da Silver no controle (entidade, partição, uri).
    ``uris_existentes``: objetos que existem de fato no bucket da Silver.
    """
    ausentes = [r for r in registradas if r["uri"] not in uris_existentes]
    por_entidade: dict[str, int] = {}
    for r in registradas:
        por_entidade[r["entidade"]] = por_entidade.get(r["entidade"], 0) + 1
    resumo = {
        "particoes_registradas": len(registradas),
        "particoes_por_entidade": por_entidade,
        "pendentes": [f"{p['fonte']} {p['particao']}" for p in pendentes],
        "ausentes_no_storage": [f"{a['entidade']} {a['particao']}" for a in ausentes],
    }

    def lista(itens: list[str]) -> str:
        return ", ".join(itens[:limite_lista]) + (f" e mais {len(itens) - limite_lista}" if len(itens) > limite_lista else "")

    falhas: list[str] = []
    if pendentes:
        falhas.append(f"{len(pendentes)} item(ns) ainda não processado(s) até a Silver: {lista(resumo['pendentes'])}")
    if ausentes:
        falhas.append(f"{len(ausentes)} partição(ões) registrada(s) sem arquivo no storage: {lista(resumo['ausentes_no_storage'])}")
    return falhas, resumo


def validar_silver_completa(settings: Settings | None = None) -> dict:
    """Portão da Gold: só deixa a carga seguir se toda a Landing já passou pela Bronze e toda a
    Bronze já passou pela Silver.

    Uma Silver incompleta (execução que falhou, que não rodou ou que ainda não chegou a um
    arquivo novo) levaria para a Gold uma competência pela metade ou desatualizada. Aqui
    isso vira uma falha explícita, com a lista do que falta; a Gold tenta de novo na
    execução seguinte.
    """
    from ans_pipeline import controle, storage
    from ans_pipeline.fontes import FONTES

    s = settings or get_settings()
    controle.aplicar_ddl(s)
    pendentes = [{"fonte": f"landing {p['fonte']}", "particao": p["particao"]} for p in controle.pendentes_bronze(list(FONTES), settings=s)]
    pendentes += [{"fonte": f"bronze {p['entidade']}", "particao": p["particao"]} for p in controle.pendentes_silver(list(FONTES), settings=s)]
    registradas = controle.particoes_silver(s)
    existentes: set[str] = set()
    for entidade in {r["entidade"] for r in registradas}:
        existentes |= {f"s3://{s.silver_bucket}/{k}" for k in storage.listar_chaves(s.silver_bucket, f"tb_{entidade}/", s)}

    falhas, resumo = avaliar_silver_completa(pendentes, registradas, existentes)
    if falhas:
        raise FalhaQualidade("Silver incompleta, nada foi carregado na Gold: " + "; ".join(falhas))
    log.info("Silver completa: %s", resumo)
    return resumo


def validar_beneficiarios(competencia: Competencia, silver_total: int, silver_ufs: int,
                          settings: Settings | None = None) -> dict:
    """Reconcilia a fato de beneficiários da competência com a Silver.

    ``silver_total`` e ``silver_ufs`` são somados pelo Spark direto da Silver, na preparação
    da Silver no PostgreSQL; a Gold é consultada depois do dbt."""
    from ans_pipeline import gold

    s = settings or get_settings()
    falhas: list[str] = []
    resultado: dict = {"competencia": competencia.iso}

    # 1) Beneficiários: total da Silver (todas as UFs) == total da fato na Gold
    gold_total, gold_ufs = gold.consultar(
        "SELECT COALESCE(sum(qt_beneficiario_ativo), 0), count(DISTINCT sg_uf) "
        "FROM gold.ft_beneficiarios WHERE sk_competencia = %s",
        (competencia.sk_data,), s,
    )[0]
    resultado.update(beneficiarios_silver=int(silver_total), beneficiarios_gold=int(gold_total), ufs=int(gold_ufs))
    if int(silver_total) != int(gold_total):
        falhas.append(f"beneficiários: Silver={silver_total} x Gold={gold_total}")
    if int(silver_ufs) != int(gold_ufs):
        falhas.append(f"UFs: Silver={silver_ufs} x Gold={gold_ufs}")

    # 2) Cobertura da dimensão produto: beneficiários em planos ausentes da base pda-008
    #    (membros inferidos). Alto demais indica problema de chave entre as fontes.
    pct_inferido = gold.consultar(
        """
        SELECT COALESCE(100.0 * sum(f.qt_beneficiario_ativo) FILTER (WHERE p.tp_origem <> 'PDA-008')
                        / NULLIF(sum(f.qt_beneficiario_ativo), 0), 0)
        FROM gold.ft_beneficiarios f JOIN gold.dm_produto p USING (sk_produto)
        WHERE f.sk_competencia = %s
        """,
        (competencia.sk_data,), s,
    )[0][0]
    resultado["pct_beneficiarios_produto_inferido"] = round(float(pct_inferido), 3)
    if pct_inferido > 5:
        falhas.append(f"{pct_inferido:.2f}% dos beneficiários sem produto correspondente na base pda-008")

    if falhas:
        raise FalhaQualidade(f"Gold reprovada (beneficiários {competencia.iso}): " + "; ".join(falhas))
    log.info("Gold aprovada: %s", resultado)
    return resultado


def validar_financeiro(ano: int, trimestre: int, silver_contas: dict[str, float],
                       settings: Settings | None = None) -> dict:
    """Soma das receitas (conta 31) e despesas assistenciais (conta 41): Silver == Gold no trimestre.
    ``silver_contas`` vem do Spark, na publicação da Silver no PostgreSQL."""
    from ans_pipeline import gold

    s = settings or get_settings()
    sk_trimestre = Competencia(ano, (trimestre - 1) * 3 + 1).sk_data
    gold_fin = gold.consultar(
        "SELECT cd_conta, sum(vl_saldo_final) FROM gold.ft_financeiro "
        "WHERE sk_trimestre = %s AND cd_conta IN ('31', '41') GROUP BY 1 ORDER BY 1",
        (sk_trimestre,), s,
    )
    silver_map = {c: float(v) for c, v in silver_contas.items()}
    gold_map = {c: float(v) for c, v in gold_fin}
    falhas: list[str] = []
    if not gold_map:
        falhas.append(f"nenhuma linha das contas 31/41 para {trimestre}T{ano}")
    for conta, valor in silver_map.items():
        if abs(valor - gold_map.get(conta, 0.0)) > 0.01:
            falhas.append(f"conta {conta}: Silver={valor:.2f} x Gold={gold_map.get(conta)}")
    resultado = {"ano": ano, "trimestre": trimestre, "contas_31_41": gold_map}
    if falhas:
        raise FalhaQualidade(f"Gold reprovada (financeiro {trimestre}T{ano}): " + "; ".join(falhas))
    log.info("Gold aprovada: %s", resultado)
    return resultado
