from enum import Enum


class Programa(Enum):
    NEWAVE = "NEWAVE"
    DECOMP = "DECOMP"
    DESSEM = "DESSEM"

    @staticmethod
    def factory(valor: str) -> "Programa":
        for p in Programa:
            if p.value == valor:
                return p
        raise ValueError(f"Programa {valor} não suportado")
