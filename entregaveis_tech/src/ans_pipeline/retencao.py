"""Retenção: o lakehouse guarda só os períodos mais recentes publicados pela ANS.

- Beneficiários: as ``MESES_RETENCAO`` competências mais recentes (padrão 3).
- Financeiro (DIOPS): os ``TRIMESTRES_RETENCAO`` trimestres mais recentes (padrão 1).
- Operadoras e produtos são cadastros (snapshots) e ficam fora da regra.

A janela é móvel: quando a ANS publica uma competência nova, a mais antiga sai.
Duas pontas aplicam a regra:

1. a Landing não baixa arquivo de período fora da janela (``filtrar_novidades``);
2. a última task da Gold apaga o que saiu da janela nas três camadas, da Gold para a
   Landing (``expurgar``). Ela roda depois da carga, quando as camadas estão consistentes.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Iterable

from ans_pipeline.config import MESES_RETENCAO, TRIMESTRES_RETENCAO, Settings, get_settings

log = logging.getLogger(__name__)

PERIODICAS = ("beneficiarios", "financeiro")
_COMPETENCIA = re.compile(r"competencia=(\d{6})")
_TRIMESTRE = re.compile(r"ano=(\d{4})/trimestre=([1-4])")


# ---------------------------------------------------------------------- regra (sem banco nem storage)
def periodo(fonte: str, caminho: str) -> str | None:
    """Período de uma partição ou chave de objeto: '202609' (competência) ou '2026-T2' (trimestre).

    None para fontes sem período (operadoras, produtos) ou caminho sem partição reconhecível."""
    if fonte == "beneficiarios" and (m := _COMPETENCIA.search(caminho)):
        return m.group(1)
    if fonte == "financeiro" and (m := _TRIMESTRE.search(caminho)):
        return f"{m.group(1)}-T{m.group(2)}"
    return None


def janela(
    periodos: dict[str, Iterable[str]], meses: int = MESES_RETENCAO, trimestres: int = TRIMESTRES_RETENCAO
) -> dict[str, set[str]]:
    """Os períodos que ficam, por fonte: os ``meses``/``trimestres`` mais recentes entre os informados.

    Os dois formatos ('AAAAMM' e 'AAAA-Tn') têm largura fixa, então a ordem do texto é a do tempo."""
    limites = {"beneficiarios": meses, "financeiro": trimestres}
    resultado: dict[str, set[str]] = {}
    for fonte, n in limites.items():
        if n < 1:
            raise ValueError(f"Retenção de {fonte} precisa ser de pelo menos 1 período (recebido {n})")
        resultado[fonte] = set(sorted(set(periodos.get(fonte, ())))[-n:])
    return resultado


def dentro(fonte: str, caminho: str, retidos: dict[str, set[str]]) -> bool:
    """O arquivo/partição fica? Fontes sem período sempre ficam."""
    p = periodo(fonte, caminho)
    return p is None or p in retidos.get(fonte, set())


def filtrar_novidades(
    novidades: list[dict], ja_na_landing: Iterable[tuple[str, str]], meses: int = MESES_RETENCAO,
    trimestres: int = TRIMESTRES_RETENCAO,
) -> tuple[list[dict], list[dict], dict[str, set[str]]]:
    """Separa as novidades da ANS em (dentro da janela, fora da janela).

    A janela considera o que já está na Landing e o que acabou de ser publicado: um mês novo
    entra e empurra o mais antigo para fora, sem que um mês já expurgado volte a ser baixado.
    """
    periodos: dict[str, set[str]] = defaultdict(set)
    for fonte, particao in ja_na_landing:
        if (p := periodo(fonte, particao)) is not None:
            periodos[fonte].add(p)
    for n in novidades:
        if (p := periodo(n["fonte"], n.get("particao", ""))) is not None:
            periodos[n["fonte"]].add(p)
    retidos = janela(periodos, meses, trimestres)
    manter = [n for n in novidades if dentro(n["fonte"], n.get("particao", ""), retidos)]
    fora = [n for n in novidades if not dentro(n["fonte"], n.get("particao", ""), retidos)]
    return manter, fora, retidos


def sk_competencia(p: str) -> int:
    """'202609' -> 20260901 (chave da dm_calendario usada na ft_beneficiarios)."""
    return int(p) * 100 + 1


def sk_trimestre(p: str) -> int:
    """'2026-T2' -> 20260401 (primeiro dia do trimestre, chave da ft_financeiro)."""
    ano, tri = int(p[:4]), int(p[-1])
    return ano * 10000 + ((tri - 1) * 3 + 1) * 100 + 1


def unidade_gold(fonte: str, p: str) -> str:
    """Nome da unidade em controle.tb_carga_gold: 'ft_beneficiarios:2026-09' ou 'ft_financeiro:2026-T2'."""
    return f"ft_beneficiarios:{p[:4]}-{p[4:]}" if fonte == "beneficiarios" else f"ft_financeiro:{p}"


# ---------------------------------------------------------------------- expurgo (o que a DAG chama)
def _apagar_objetos(bucket: str, chaves: list[str], settings: Settings) -> None:
    from ans_pipeline import storage

    cliente = storage.s3_client(settings)
    for i in range(0, len(chaves), 1000):
        lote = chaves[i:i + 1000]
        resp = cliente.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in lote], "Quiet": True})
        if resp.get("Errors"):
            raise IOError(f"Falha ao apagar {len(resp['Errors'])} objeto(s) de {bucket}: {resp['Errors'][:3]}")


def expurgar(settings: Settings | None = None) -> dict:
    """Apaga, nas três camadas, os períodos que saíram da janela de retenção.

    A janela vem do controle da Landing (o que foi ingerido). Ordem: Gold e tabelas de
    controle numa transação; depois os objetos da Silver, da Bronze e da Landing. Os objetos são
    procurados no storage, não só no controle, então sobras de execuções antigas também saem.
    Se uma fonte não tem nenhum período no controle, nada dela é apagado.
    """
    import psycopg2

    from ans_pipeline import controle, storage

    s = settings or get_settings()
    controle.aplicar_ddl(s)
    periodos: dict[str, set[str]] = defaultdict(set)
    for fonte, particao in controle.ultimos_landing(s):
        if (p := periodo(fonte, particao)) is not None:
            periodos[fonte].add(p)
    fontes = [f for f in PERIODICAS if periodos[f]]
    retidos = janela(periodos)
    saem: dict[str, set[str]] = {f: periodos[f] - retidos[f] for f in fontes}

    # 1) Gold e controle, numa transação
    linhas: dict[str, int] = {}
    conn = psycopg2.connect(s.dw_dsn)
    try:
        with conn, conn.cursor() as cur:
            def apagar(rotulo: str, sql: str, params: tuple) -> None:
                cur.execute(sql, params)
                linhas[rotulo] = linhas.get(rotulo, 0) + cur.rowcount

            for fonte in fontes:
                ficam = sorted(retidos[fonte])
                if fonte == "beneficiarios":
                    apagar("gold.ft_beneficiarios",
                           "DELETE FROM gold.ft_beneficiarios WHERE sk_competencia <> ALL(%s)",
                           ([sk_competencia(p) for p in ficam],))
                else:
                    apagar("gold.ft_financeiro",
                           "DELETE FROM gold.ft_financeiro WHERE sk_trimestre <> ALL(%s)",
                           ([sk_trimestre(p) for p in ficam],))
                # Silver publicada no PostgreSQL (schema silver), se a Gold já rodou alguma vez
                cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"silver.tb_{fonte}",))
                if cur.fetchone()[0]:
                    coluna = "dt_competencia" if fonte == "beneficiarios" else "dt_trimestre"
                    datas = [str(sk_competencia(p) if fonte == "beneficiarios" else sk_trimestre(p)) for p in ficam]
                    apagar(f"silver.tb_{fonte}",
                           f"DELETE FROM silver.tb_{fonte} WHERE {coluna} <> ALL(%s::date[])",
                           ([f"{d[:4]}-{d[4:6]}-{d[6:]}" for d in datas],))
                prefixo = "ft_beneficiarios:" if fonte == "beneficiarios" else "ft_financeiro:"
                apagar("controle.tb_carga_gold",
                       "DELETE FROM controle.tb_carga_gold WHERE cd_unidade LIKE %s AND cd_unidade <> ALL(%s)",
                       (prefixo + "%", [unidade_gold(fonte, p) for p in ficam]))
                # partições da Silver e da Bronze e arquivos da Landing: a regra é a mesma de ``dentro``
                cur.execute("SELECT nm_entidade, ds_particao FROM controle.tb_particao_silver WHERE nm_entidade = %s", (fonte,))
                fora = [pt for _, pt in cur.fetchall() if not dentro(fonte, pt, retidos)]
                if fora:
                    apagar("controle.tb_particao_silver",
                           "DELETE FROM controle.tb_particao_silver WHERE nm_entidade = %s AND ds_particao = ANY(%s)",
                           (fonte, fora))
                cur.execute("SELECT nm_entidade, ds_particao FROM controle.tb_particao_bronze WHERE nm_entidade = %s", (fonte,))
                fora = [pt for _, pt in cur.fetchall() if not dentro(fonte, pt, retidos)]
                if fora:
                    apagar("controle.tb_particao_bronze",
                           "DELETE FROM controle.tb_particao_bronze WHERE nm_entidade = %s AND ds_particao = ANY(%s)",
                           (fonte, fora))
                cur.execute("SELECT ds_chave, ds_particao FROM controle.tb_arquivo_landing WHERE nm_fonte = %s", (fonte,))
                fora = [ch for ch, pt in cur.fetchall() if not dentro(fonte, pt, retidos)]
                if fora:
                    apagar("controle.tb_arquivo_landing",
                           "DELETE FROM controle.tb_arquivo_landing WHERE ds_chave = ANY(%s)", (fora,))
    finally:
        conn.close()

    # 2) objetos: Silver (dados e quarentena), Bronze e Landing
    objetos: dict[str, int] = {}
    for bucket, prefixos in (
        (s.silver_bucket, [(f, f"tb_{f}/") for f in fontes] + [(f, f"_quarentena/tb_{f}/") for f in fontes]),
        (s.bronze_bucket, [(f, f"{f}/") for f in fontes]),
        (s.landing_bucket, [(f, f"{f}/") for f in fontes]),
    ):
        chaves = []
        for fonte, prefixo in prefixos:
            for k in storage.listar_chaves(bucket, prefixo, s):
                if not dentro(fonte, k, retidos):
                    chaves.append(k)
                    saem[fonte].add(periodo(fonte, k))
        _apagar_objetos(bucket, chaves, s)
        objetos[bucket] = len(chaves)

    resultado = {
        "retidos": {f: sorted(retidos[f]) for f in fontes},
        "expurgados": {f: sorted(saem[f]) for f in fontes},
        "linhas_apagadas": linhas,
        "objetos_apagados": objetos,
    }
    log.info("Retenção aplicada: %s", resultado)
    return resultado
