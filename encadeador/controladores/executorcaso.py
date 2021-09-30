from abc import abstractmethod
from os import chdir
from logging import Logger
from typing import Optional
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso

from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.controladores.preparadorcaso import PreparadorCaso
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorCasoNEWAVE


class ExecutorCaso:

    def __init__(self,
                 caso: Caso,
                 log: Logger) -> None:
        self._caso = caso
        self._log = log
        self._preparador = PreparadorCaso.factory(caso, log)
        self._monitor = MonitorCaso.factory(caso, log)
        self._armazenador = ArmazenadorCaso(caso, log)
        self._sintetizador = SintetizadorCaso.factory(caso, log)

    @staticmethod
    def factory(caso: Caso,
                log: Logger) -> 'ExecutorCaso':
        if isinstance(caso, CasoNEWAVE):
            return ExecutorNEWAVE(caso, log)
        elif isinstance(caso, CasoDECOMP):
            return ExecutorDECOMP(caso, log)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    @abstractmethod
    def executa_e_monitora_caso(self, **kwargs) -> bool:
        pass


class ExecutorNEWAVE(ExecutorCaso):

    def __init__(self,
                 caso: CasoNEWAVE,
                 log: Logger) -> None:
        super().__init__(caso,
                         log)

    def executa_e_monitora_caso(self, **kwargs) -> bool:

        chdir(self._caso.caminho)

        ultimo_newave: Optional[CasoNEWAVE] = kwargs.get("ultimo_newave")
        ultimo_decomp: Optional[CasoNEWAVE] = kwargs.get("ultimo_decomp")
        # Se não é o primeiro NEWAVE, apaga os cortes do último
        if ultimo_newave is not None:
            sint_ultimo = SintetizadorCasoNEWAVE(ultimo_newave,
                                                  self._log)
            if sint_ultimo.verifica_cortes_extraidos():
                sint_ultimo.deleta_cortes()

        if not self._preparador.prepara_caso():
            raise RuntimeError(f"Erro na preparação do NW {self._caso.nome}")
        if not self._preparador.encadeia_variaveis(ultimo_decomp):
            raise RuntimeError(f"Erro no encadeamento do NW {self._caso.nome}")
        if not self._monitor.executa_caso():
            raise RuntimeError(f"Erro na execução do NW {self._caso.nome}")

        ret = True
        if not self._sintetizador.sintetiza_caso():
            self._log.error(f"Erro ao sintetizar caso: {self._caso.nome}")
            ret = False
        if not self._armazenador.armazena_caso(self._caso.estado):
            self._log.error(f"Erro ao armazenar caso: {self._caso.nome}")
            ret = False
        return ret


class ExecutorDECOMP(ExecutorCaso):

    def __init__(self,
                 caso: CasoDECOMP,
                 log: Logger) -> None:
        super().__init__(caso,
                         log)

    def executa_e_monitora_caso(self, **kwargs) -> bool:

        chdir(self._caso.caminho)

        ultimo_newave: Optional[CasoNEWAVE] = kwargs.get("ultimo_newave")
        ultimo_decomp: Optional[CasoNEWAVE] = kwargs.get("ultimo_decomp")

        if not self._preparador.prepara_caso(caso_cortes=ultimo_newave):
            raise RuntimeError(f"Erro na preparação do DC {self._caso.nome}")
        if not self._preparador.encadeia_variaveis(ultimo_decomp):
            raise RuntimeError(f"Erro no encadeamento do DC {self._caso.nome}")
        if not self._monitor.executa_caso():
            raise RuntimeError(f"Erro na execução do DC {self._caso.nome}")

        ret = True
        if not self._sintetizador.sintetiza_caso():
            self._log.error(f"Erro ao sintetizar caso: {self._caso.nome}")
            ret = False
        if not self._armazenador.armazena_caso(self._caso.estado):
            self._log.error(f"Erro ao armazenar caso: {self._caso.nome}")
            ret = False
        return ret
