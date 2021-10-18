from abc import abstractmethod
from os import chdir
from logging import Logger
from typing import Optional
from encadeador.controladores.avaliadorcaso import AvaliadorCaso
from encadeador.controladores.flexibilizadorcaso import Flexibilizador

from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.controladores.preparadorcaso import PreparadorCaso
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorNEWAVE


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
        self._avaliador = AvaliadorCaso.factory(caso, log)

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
    def executa_e_monitora_caso(self,
                                ultimo_newave: Optional[CasoNEWAVE],
                                ultimo_decomp: Optional[CasoDECOMP]) -> bool:
        pass


class ExecutorNEWAVE(ExecutorCaso):

    def __init__(self,
                 caso: CasoNEWAVE,
                 log: Logger) -> None:
        super().__init__(caso,
                         log)

    def executa_e_monitora_caso(self,
                                ultimo_newave: Optional[CasoNEWAVE],
                                ultimo_decomp: Optional[CasoDECOMP]) -> bool:

        chdir(self._caso.caminho)

        # Se não é o primeiro NEWAVE, apaga os cortes do último
        if ultimo_newave is not None:
            sint_ultimo = SintetizadorNEWAVE(ultimo_newave,
                                             self._log)
            if sint_ultimo.verifica_cortes_extraidos():
                sint_ultimo.deleta_cortes()

        if not self._preparador.prepara_caso():
            raise RuntimeError(f"Erro na preparação do NW {self._caso.nome}")
        if not self._preparador.encadeia_variaveis(ultimo_decomp):
            raise RuntimeError(f"Erro no encadeamento do NW {self._caso.nome}")
        if not self._monitor.executa_caso():
            raise RuntimeError(f"Erro na execução do NW {self._caso.nome}")

        # Se o NEWAVE não obteve sucesso, está com erro
        if not self._caso.sucesso:
            raise RuntimeError(f"Erro nas saídas do NW {self._caso.nome}")

        ret = True
        if not self._sintetizador.sintetiza_caso():
            self._log.error(f"Erro ao sintetizar caso: {self._caso.nome}")
            ret = False
        if not self._armazenador.armazena_caso():
            self._log.error(f"Erro ao armazenar caso: {self._caso.nome}")
            ret = False
        return ret


class ExecutorDECOMP(ExecutorCaso):

    def __init__(self,
                 caso: CasoDECOMP,
                 log: Logger) -> None:
        super().__init__(caso,
                         log)

    def executa_e_monitora_caso(self,
                                ultimo_newave: Optional[CasoNEWAVE],
                                ultimo_decomp: Optional[CasoDECOMP]) -> bool:

        chdir(self._caso.caminho)

        if not self._preparador.prepara_caso(caso_cortes=ultimo_newave):
            raise RuntimeError(f"Erro na preparação do DC {self._caso.nome}")
        if not self._preparador.encadeia_variaveis(ultimo_decomp):
            raise RuntimeError(f"Erro no encadeamento do DC {self._caso.nome}")
        # Primeira execução: obrigatória
        if not self._monitor.executa_caso():
            raise RuntimeError(f"Erro na execução do DC {self._caso.nome}")
        # Execuções adicionais: se necessárias
        while not self._caso.sucesso:
            max_flex = self._caso.configuracoes.maximo_flexibilizacoes_revisao
            if self._caso.numero_flexibilizacoes >= max_flex:
                raise RuntimeError("Máximo de flexibilizações atingido" +
                                   f" no DC {self._caso.nome}")
            # Se ainda pode flexibilizar
            flexibilizador = Flexibilizador.factory(self._caso, self._log)
            if not flexibilizador.flexibiliza():
                raise RuntimeError("Erro na flexibilização do " +
                                   f"DC {self._caso.nome}")
            if not self._monitor.executa_caso():
                raise RuntimeError(f"Erro na execução do DC {self._caso.nome}")

        ret = True
        if not self._sintetizador.sintetiza_caso():
            self._log.error(f"Erro ao sintetizar caso: {self._caso.nome}")
            ret = False
        if not self._armazenador.armazena_caso():
            self._log.error(f"Erro ao armazenar caso: {self._caso.nome}")
            ret = False
        return ret
