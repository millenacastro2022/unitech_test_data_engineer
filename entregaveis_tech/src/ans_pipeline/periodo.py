"""Competência (mês de referência), trimestre e as janelas que a Bronze verifica na ANS."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, datetime

_PADRAO = re.compile(r"^(?P<ano>\d{4})-?(?P<mes>0[1-9]|1[0-2])$")


@dataclass(frozen=True, order=True)
class Competencia:
    ano: int
    mes: int

    def __post_init__(self) -> None:
        if not 1 <= self.mes <= 12:
            raise ValueError(f"Mês inválido: {self.mes}")
        if not 2000 <= self.ano <= 2100:
            raise ValueError(f"Ano fora do intervalo suportado: {self.ano}")

    # ------------------------------------------------------------------ construtores
    @classmethod
    def parse(cls, valor: str) -> "Competencia":
        """Aceita 'AAAA-MM' ou 'AAAAMM'."""
        m = _PADRAO.match(str(valor).strip())
        if not m:
            raise ValueError(f"Competência inválida '{valor}'. Use o formato AAAA-MM.")
        return cls(int(m["ano"]), int(m["mes"]))

    @classmethod
    def from_date(cls, d: date | datetime) -> "Competencia":
        return cls(d.year, d.month)

    # ------------------------------------------------------------------ propriedades
    @property
    def yyyymm(self) -> str:
        return f"{self.ano}{self.mes:02d}"

    @property
    def iso(self) -> str:
        return f"{self.ano}-{self.mes:02d}"

    @property
    def primeiro_dia(self) -> date:
        return date(self.ano, self.mes, 1)

    @property
    def sk_data(self) -> int:
        """Chave da dm_calendario (AAAAMMDD) do primeiro dia do mês."""
        return self.ano * 10000 + self.mes * 100 + 1

    @property
    def trimestre(self) -> int:
        return (self.mes - 1) // 3 + 1

    @property
    def fim_de_trimestre(self) -> bool:
        return self.mes % 3 == 0

    @property
    def inicio_trimestre(self) -> date:
        return date(self.ano, (self.trimestre - 1) * 3 + 1, 1)

    @property
    def sk_trimestre(self) -> int:
        d = self.inicio_trimestre
        return d.year * 10000 + d.month * 100 + 1

    def somar(self, meses: int) -> "Competencia":
        total = self.ano * 12 + (self.mes - 1) + meses
        return Competencia(total // 12, total % 12 + 1)

    def __str__(self) -> str:
        return self.iso


# ---------------------------------------------------------------------- janelas de verificação
def janela_competencias(referencia: date, meses: int) -> list[Competencia]:
    """Os ``meses`` meses fechados antes de ``referencia`` (o mais antigo primeiro).

    A ANS publica cada competência semanas depois do fim do mês e às vezes republica
    meses anteriores; por isso a Bronze olha uma janela, não um mês só."""
    atual = Competencia.from_date(referencia)
    return [atual.somar(-n) for n in range(meses, 0, -1)]


def trimestres_fechados(referencia: date, quantidade: int) -> list[tuple[int, int]]:
    """Os ``quantidade`` trimestres já encerrados antes de ``referencia`` (o mais antigo primeiro)."""
    atual = Competencia.from_date(referencia)
    ultimo_fechado = atual.somar(-((atual.mes - 1) % 3) - 1)  # último mês do trimestre anterior
    return [(c.ano, c.trimestre) for c in (ultimo_fechado.somar(-3 * n) for n in range(quantidade - 1, -1, -1))]


def agrupar_por_periodo(itens: list[dict], campo: str = "fonte") -> list[list[dict]]:
    """Agrupa itens de uma fila por fonte/entidade e período (ex.: as 28 UFs de uma competência):
    cada grupo vira um job Spark, que paga a partida da JVM uma vez só."""
    grupos: dict[tuple[str, str], list[dict]] = {}
    for i in itens:
        grupos.setdefault((i[campo], i["particao"].split("/uf=")[0]), []).append(i)
    return [sorted(grupos[k], key=lambda i: i["particao"]) for k in sorted(grupos)]
