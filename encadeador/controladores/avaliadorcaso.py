from abc import abstractmethod
from os import chdir
from logging import Logger

from encadeador.modelos.arvorecasos import ArvoreCasos
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.controladores.preparadorcaso import PreparadorCasoNEWAVE
from encadeador.controladores.preparadorcaso import PreparadorCasoDECOMP
from encadeador.controladores.monitorcaso import MonitorNEWAVE
from encadeador.controladores.monitorcaso import MonitorDECOMP
from encadeador.controladores.sintetizadorcaso import SintetizadorCasoNEWAVE


class AvaliadorCaso:

    def __init__(self,
                 caso: Caso,
                 log: Logger) -> None:
        self._caso = caso
        self._cfg = cfg
        self._log = log

    @staticmethod
    def factory(caso: Caso,
                log: Logger) -> 'AvaliadorCaso':
        if isinstance(caso, CasoNEWAVE):
            return AvaliadorCasoNEWAVE(caso, log)
        elif isinstance(caso, CasoDECOMP):
            return AvaliadorCasoDECOMP(caso, log)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    @abstractmethod
    def executa_e_monitora_caso(self) -> bool:
        pass


class AvaliadorCasoNEWAVE(AvaliadorCaso):

    def __init__(self,
                 caso: CasoNEWAVE,
                 log: Logger) -> None:
        super().__init__(caso,
                         cfg,
                         log)

    def executa_e_monitora_caso(self) -> bool:
        pass


class AvaliadorCasoDECOMP(AvaliadorCaso):

    def __init__(self,
                 caso: CasoDECOMP,
                 log: Logger) -> None:
        super().__init__(caso,
                         cfg,
                         log)

    def executa_e_monitora_caso(self) -> bool:
        pass
