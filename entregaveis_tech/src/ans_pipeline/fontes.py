"""Catálogo das fontes de dados abertos da ANS (https://dadosabertos.ans.gov.br/FTP/PDA/)."""

from __future__ import annotations

from dataclasses import dataclass

from ans_pipeline.periodo import Competencia


FONTES: tuple[str, ...] = ("operadoras", "produtos", "beneficiarios", "financeiro")


@dataclass(frozen=True)
class ArquivoFonte:
    fonte: str          # nome lógico (também é o prefixo na Landing)
    url: str
    particao: str = ""  # sub-partição na Landing além da data de ingestão (ex.: competencia=202609)
    uf: str = ""        # só beneficiários: um arquivo por UF

    @property
    def nome_arquivo(self) -> str:
        return self.url.rsplit("/", 1)[-1]

    @property
    def particao_controle(self) -> str:
        """Partição lógica de um arquivo da ANS, usada nas tabelas de controle.
        Um arquivo mais novo da mesma partição substitui o anterior na Silver."""
        if self.uf:
            return f"{self.particao}/uf={self.uf}"
        return self.particao or "snapshot"

    @property
    def rotulo(self) -> str:
        return f"{self.fonte} {self.particao_controle}"

    def to_dict(self) -> dict:
        return {"fonte": self.fonte, "url": self.url, "particao": self.particao, "uf": self.uf}

    @classmethod
    def from_dict(cls, d: dict) -> "ArquivoFonte":
        return cls(fonte=d["fonte"], url=d["url"], particao=d.get("particao", ""), uf=d.get("uf", ""))


def operadoras(base_url: str) -> ArquivoFonte:
    """Relatório do Cadastro de Operadoras Ativas (CADOP) – snapshot diário, CSV."""
    return ArquivoFonte(
        fonte="operadoras",
        url=f"{base_url}/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv",
    )


def produtos(base_url: str) -> ArquivoFonte:
    """Características dos produtos (planos) de saúde – pda-008, snapshot diário, CSV."""
    return ArquivoFonte(
        fonte="produtos",
        url=(
            f"{base_url}/caracteristicas_produtos_saude_suplementar-008/"
            "pda-008-caracteristicas_produtos_saude_suplementar.csv"
        ),
    )


def beneficiarios(base_url: str, competencia: Competencia, uf: str) -> ArquivoFonte:
    """Informações consolidadas de beneficiários (pda-024) – mensal, um ZIP por UF.

    É a base de microdados agregados que alimenta o Caderno de Informação da Saúde
    Suplementar (veja a seção "Decisões de design" do README).
    """
    uf = uf.upper()
    return ArquivoFonte(
        fonte="beneficiarios",
        url=(
            f"{base_url}/informacoes_consolidadas_de_beneficiarios-024/{competencia.yyyymm}/"
            f"pda-024-icb-{uf}-{competencia.ano}_{competencia.mes:02d}.zip"
        ),
        particao=f"competencia={competencia.yyyymm}",
        uf=uf,
    )


def financeiro(base_url: str, ano: int, trimestre: int) -> ArquivoFonte:
    """Demonstrações contábeis (DIOPS) – trimestral, ZIP com um CSV."""
    return ArquivoFonte(
        fonte="financeiro",
        url=f"{base_url}/demonstracoes_contabeis/{ano}/{trimestre}T{ano}.zip",
        particao=f"ano={ano}/trimestre={trimestre}",
    )


def candidatos(
    base_url: str,
    competencias: list[Competencia],
    trimestres: list[tuple[int, int]],
    ufs: list[str],
    fontes_escolhidas: list[str],
) -> list[ArquivoFonte]:
    """Todos os arquivos que a Landing deve verificar numa execução."""
    lista: list[ArquivoFonte] = []
    if "operadoras" in fontes_escolhidas:
        lista.append(operadoras(base_url))
    if "produtos" in fontes_escolhidas:
        lista.append(produtos(base_url))
    if "beneficiarios" in fontes_escolhidas:
        lista += [beneficiarios(base_url, c, uf) for c in competencias for uf in ufs]
    if "financeiro" in fontes_escolhidas:
        lista += [financeiro(base_url, ano, tri) for ano, tri in trimestres]
    return lista
