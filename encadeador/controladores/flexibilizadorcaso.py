from abc import abstractmethod
from logging import Logger

from encadeador.modelos.caso import Caso, CasoDECOMP


class Flexibilizador:

    def __init__(self,
                 caso: Caso,
                 log: Logger):
        self._caso = caso
        self._log = log

    @staticmethod
    def factory(caso: Caso,
                log: Logger) -> 'Flexibilizador':
        if isinstance(caso, CasoDECOMP):
            return FlexibilizadorDECOMP(caso,
                                        log)
        else:
            raise TypeError(f"Caso do tipo {type(caso)} " +
                            "não suportado para encadeamento")

    @abstractmethod
    def flexibiliza(self) -> bool:
        pass


class FlexibilizadorDECOMP(Flexibilizador):

    def __init__(self,
                 caso: Caso,
                 log: Logger):
        super().__init__(caso, log)

    def flexibiliza(self) -> bool:
        self._caso.adiciona_flexibilizacao()
        # TODO - Lê o inviab_unic.rvX
        # TODO - Lê o dadger.rvX
        # TODO - Faz as flexibilizações
        # TODO - Escreve o dadger.rvX
        return True
