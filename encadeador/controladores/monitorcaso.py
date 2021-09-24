from abc import abstractmethod
from typing import List, Tuple
from logging import Logger
from os.path import join
from os import listdir
import time

from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.modelos.estadojob import EstadoJob
from encadeador.controladores.gerenciadorfila import GerenciadorFila


# STUB para o objeto verdadeiro
from encadeador.modelos.caso import Configuracoes


class MonitorCaso:
    """
    Responsável por executar e monitorar a execução
    de um caso em um gerenciador de filas.
    """
    INTERVALO_POLL = 5.0
    def __init__(self, caso: Caso):
        self._caso = caso
        g = caso.configuracoes.gerenciador_fila
        self._gerenciador = GerenciadorFila.factory(g)

    @abstractmethod
    def executa_caso(self, log: Logger) -> bool:
        pass

    @property
    def caso(self) -> Caso:
        return self._caso


class MonitorNEWAVE(MonitorCaso):

    TIMEOUT_COMUNICACAO = 1800
    MAX_RETRY = 3

    def __init__(self, caso: CasoNEWAVE):
        super().__init__(caso)
        self._caminho_job = self.__obtem_caminho_job()
        self._nome_job = self.__obtem_nome_job()

    @property
    def caso(self) -> CasoNEWAVE:
        return self._caso

    def __obtem_caminho_job(self) -> str:
        cfg = self.caso.configuracoes
        dir_base = cfg.diretorio_instalacao_newaves
        versao = cfg.versao_newave
        dir_versao = join(dir_base, versao)
        arquivos_versao = listdir(dir_versao)
        arq_job = [a for a in arquivos_versao if ".job" in a]
        return join(dir_versao, arq_job[0])

    def __obtem_nome_job(self) -> str:
        return f"NW{self.caso.ano}{self.caso.mes}"

    def _trata_caso_retry(self, log: Logger):
        if self.caso.numero_tentativas >= MonitorNEWAVE.MAX_RETRY:
            raise ValueError(f"Caso {self.caso.nome} com falha" +
                             f"após {MonitorNEWAVE.MAX_RETRY}" +
                             " tentativas")
        log.info("Reagendando job para o caso:" +
                 f" {self.caso._nome_caso}")
        self.caso.coloca_caso_na_fila()
        self._gerenciador.agenda_job(self._caminho_job,
                                     self._nome_job,
                                     self.caso.numero_processadores)

    def _trata_caso_executando(self,
                               log: Logger,
                               iniciou: bool) -> Tuple[bool, bool]:
        retry = False
        iniciou = False
        if (self._gerenciador.tempo_job_idle >
            MonitorNEWAVE.TIMEOUT_COMUNICACAO):
            log.info(f"Erro de comunicacao no caso: {self.caso.nome}.")
            s = self._gerenciador.deleta_job()
            if not s:
                raise ValueError("Erro ao deletar o job " +
                                    f"{self.caso.nome}")
            retry = True
        if not iniciou:
            iniciou = True
            self.caso.inicia_caso()
            log.info(f"Iniciando execução do caso: {self.caso.nome}")
        return retry, iniciou

    def _trata_caso_erro(self,
                         log: Logger) -> bool:
        log.error(f"Erro na execução do caso: {self.caso.nome}")
        s = self._gerenciador.deleta_job()
        if not s:
            raise ValueError("Erro ao deletar o job " +
                             f"{self.caso.nome}")
        return True

    def executa_caso(self, log: Logger) -> bool:
        try:
            # Inicia a execução do job
            log.info(f"Agendando job para o caso: {self.caso.nome}")
            self.caso.inicializa_parametros_execucao()
            self.caso.coloca_caso_na_fila()
            self._gerenciador.agenda_job(self._caminho_job,
                                         self._nome_job,
                                         self.caso.numero_processadores)
            ultimo_estado = EstadoJob.ESPERANDO
            retry = False
            iniciou = False
            while True:
                # Execução rápida do retry, se precisar
                if retry:
                    self._trata_caso_retry(log)
                    retry = False
                # Máquina de estados para controlar a execução
                estado = self._gerenciador.estado_job
                if estado == EstadoJob.ESPERANDO:
                    pass
                elif estado == EstadoJob.EXECUTANDO:
                    retry, iniciou = self._trata_caso_executando(log,
                                                                 iniciou)
                elif estado == EstadoJob.DELETANDO:
                    pass
                elif estado == EstadoJob.ERRO:
                    retry = self._trata_caso_erro(log)
                ultimo_estado = estado
                time.sleep(MonitorNEWAVE.INTERVALO_POLL)
        except TimeoutError as e:
            log.error(f"Timeout na execução do job {self.caso.nome}: {e}")
            return False
        except ValueError as e:
            log.error(f"Erro na execução do job {self.caso.nome}: {e}")
            return False
        except KeyError as e:
            if ultimo_estado == EstadoJob.EXECUTANDO:
                return True
            else:
                log.error(f"Erro na execução do job {self.caso.nome}: {e}")
                return False


class MonitorDECOMP(MonitorCaso):

    TIMEOUT_COMUNICACAO = 300
    MAX_RETRY = 3

    def __init__(self, caso: CasoDECOMP):
        super().__init__(caso)
        self._caminho_job = self.__obtem_caminho_job()
        self._nome_job = self.__obtem_nome_job()

    @property
    def caso(self) -> CasoDECOMP:
        return self._caso

    def __obtem_caminho_job(self) -> str:
        cfg = self.caso.configuracoes
        dir_base = cfg.diretorio_instalacao_decomps
        versao = cfg.versao_newave
        dir_versao = join(dir_base, versao)
        arquivos_versao = listdir(dir_versao)
        arq_job = [a for a in arquivos_versao if ".job" in a]
        return join(dir_versao, arq_job[0])

    def __obtem_nome_job(self) -> str:
        return f"NW{self.caso.ano}{self.caso.mes}"

    def _trata_caso_retry(self, log: Logger):
        if self.caso.numero_tentativas >= MonitorDECOMP.MAX_RETRY:
            raise ValueError(f"Caso {self.caso.nome} com falha" +
                             f"após {MonitorDECOMP.MAX_RETRY}" +
                             " tentativas")
        log.info("Reagendando job para o caso:" +
                 f" {self.caso._nome_caso}")
        self.caso.coloca_caso_na_fila()
        self._gerenciador.agenda_job(self._caminho_job,
                                     self._nome_job,
                                     self.caso.numero_processadores)

    def _trata_caso_executando(self,
                               log: Logger,
                               iniciou: bool) -> Tuple[bool, bool]:
        retry = False
        iniciou = False
        if (self._gerenciador.tempo_job_idle >
            MonitorDECOMP.TIMEOUT_COMUNICACAO):
            log.info(f"Erro de comunicacao no caso: {self.caso.nome}.")
            s = self._gerenciador.deleta_job()
            if not s:
                raise ValueError("Erro ao deletar o job " +
                                    f"{self.caso.nome}")
            retry = True
        if not iniciou:
            iniciou = True
            self.caso.inicia_caso()
            log.info(f"Iniciando execução do caso: {self.caso.nome}")
        return retry, iniciou

    def _trata_caso_erro(self,
                         log: Logger) -> bool:
        log.error(f"Erro na execução do caso: {self.caso.nome}")
        s = self._gerenciador.deleta_job()
        if not s:
            raise ValueError("Erro ao deletar o job " +
                             f"{self.caso.nome}")
        return True

    def executa_caso(self, log: Logger) -> bool:
        try:
            # Inicia a execução do job
            log.info(f"Agendando job para o caso: {self.caso.nome}")
            self.caso.inicializa_parametros_execucao()
            self.caso.coloca_caso_na_fila()
            self._gerenciador.agenda_job(self._caminho_job,
                                         self._nome_job,
                                         self.caso.numero_processadores)
            ultimo_estado = EstadoJob.ESPERANDO
            retry = False
            iniciou = False
            while True:
                # Execução rápida do retry, se precisar
                if retry:
                    self._trata_caso_retry(log)
                    retry = False
                # Máquina de estados para controlar a execução
                estado = self._gerenciador.estado_job
                if estado == EstadoJob.ESPERANDO:
                    pass
                elif estado == EstadoJob.EXECUTANDO:
                    retry, iniciou = self._trata_caso_executando(log,
                                                                 iniciou)
                elif estado == EstadoJob.DELETANDO:
                    pass
                elif estado == EstadoJob.ERRO:
                    retry = self._trata_caso_erro(log)
                ultimo_estado = estado
                time.sleep(MonitorDECOMP.INTERVALO_POLL)
        except TimeoutError as e:
            log.error(f"Timeout na execução do job {self.caso.nome}: {e}")
            return False
        except ValueError as e:
            log.error(f"Erro na execução do job {self.caso.nome}: {e}")
            return False
        except KeyError as e:
            if ultimo_estado == EstadoJob.EXECUTANDO:
                return True
            else:
                log.error(f"Erro na execução do job {self.caso.nome}: {e}")
                return False
