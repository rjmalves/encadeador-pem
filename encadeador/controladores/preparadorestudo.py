from abc import abstractmethod

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.estudo import Estudo
from encadeador.modelos.regrareservatorio import RegraReservatorio


class PreparadorEstudo:
    def __init__(self, estudo: Estudo) -> None:
        self._estudo = estudo

    def prepara_estudo(self) -> bool:
        return self.carrega_regras_operacao_reservatorios()

    @abstractmethod
    def carrega_regras_operacao_reservatorios(self) -> bool:
        regras_reserv = Configuracoes().arquivo_regras_operacao_reservatorios
        try:
            if regras_reserv is not None:
                self._estudo._regras_reservatorio = RegraReservatorio.from_csv(
                    regras_reserv
                )
            else:
                self._estudo._regras_reservatorio = []
        except FileNotFoundError:
            return False
        return True

    def carrega_regras_flexibilizacao_inviabilidades(self) -> bool:
        pass

    @property
    def estudo(self) -> Estudo:
        return self._estudo
