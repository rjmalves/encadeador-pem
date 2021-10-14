from abc import abstractmethod
from logging import Logger

from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP


class Encadeador:

    def __init__(self,
                 caso_anterior: Caso,
                 caso_atual: Caso,
                 log: Logger):
        self._caso_anterior = caso_anterior
        self._caso_atual = caso_atual
        self._log = log

    @staticmethod
    def factory(caso_anterior: Caso,
                caso_atual: Caso,
                log: Logger) -> 'Encadeador':
        if isinstance(caso_atual, CasoDECOMP):
            return EncadeadorDECOMPDECOMP(caso_anterior,
                                          caso_atual,
                                          log)
        elif isinstance(caso_atual, CasoNEWAVE):
            return EncadeadorDECOMPNEWAVE(caso_anterior,
                                          caso_atual,
                                          log)
        else:
            raise TypeError(f"Caso do tipo {type(caso_atual)} " +
                             "não suportado para encadeamento")

    @abstractmethod
    def encadeia(self) -> bool:
        pass


class EncadeadorDECOMPNEWAVE(Encadeador):

    def __init__(self,
                 caso_anterior: Caso,
                 caso_atual: Caso,
                 log: Logger):
        super().__init__(caso_anterior, caso_atual, log)

    def encadeia(self) -> bool:
        return True


class EncadeadorDECOMPDECOMP(Encadeador):

    def __init__(self,
                 caso_anterior: Caso,
                 caso_atual: Caso,
                 log: Logger):
        super().__init__(caso_anterior, caso_atual, log)

    def encadeia(self) -> bool:
        return True
