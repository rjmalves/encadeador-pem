from pydantic import BaseModel
from typing import Optional

from encadeador.modelos.regrareservatorio import RegraReservatorio


class ReservoirRule(BaseModel):
    """
    Class for defining a reservoir operation rule
    based on other states.
    """

    reservoirCode: int
    uheCode: int
    constraintType: str
    month: int
    minVolume: float
    maxVolume: float
    minLimit: Optional[float]
    maxLimit: Optional[float]
    frequency: str
    label: Optional[str]

    def __str__(self) -> str:
        return (
            f"Regra: reservatório {self.reservoirCode}"
            + f" -> usina {self.uheCode} | {self.constraintType}"
            + f" mês {self.month}. Faixa {self.label}: "
            + f" ({self.minVolume}, {self.maxVolume})"
            + f" -> ({self.minLimit},{self.maxLimit}). "
            + f" Periodicidade {self.frequency}"
        )

    def __key(self):
        return (
            self.reservoirCode,
            self.uheCode,
            self.constraintType,
            self.month,
            self.label,
            self.minVolume,
            self.maxVolume,
            self.minLimit,
            self.maxLimit,
        )

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, ReservoirRule):
            return self.__key() == other.__key()
        return NotImplemented

    @classmethod
    def from_regra(cls, regra: RegraReservatorio) -> "ReservoirRule":
        return cls(
            reservoirCode=regra.codigo_reservatorio,
            uheCode=regra.codigo_usina,
            constraintType=regra.tipo_restricao,
            month=regra.mes,
            label=regra.legenda_faixa,
            minVolume=regra.volume_minimo,
            maxVolume=regra.volume_maximo,
            minLimit=regra.limite_minimo,
            maxLimit=regra.limite_maximo,
        )
