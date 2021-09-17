from abc import abstractmethod
from typing import List, Tuple
from logging import Logger
import time

from encadeador.modelos.caso import Caso
from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.gerenciadorfila import GerenciadorFila


# STUB para o objeto verdadeiro
class Configuracoes:
    def __init__(self) -> None:
        self.gerenciador_fila = "SGE"
    pass


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
    def executa_caso(self, comando: List[str]) -> bool:
        pass

    @property
    def caso(self) -> Caso:
        return self._caso


class MonitorNEWAVE(MonitorCaso):
    TIMEOUT_COMUNICACAO = 1800
    MAX_RETRY = 3
    def __init__(self, caso: Caso):
        super().__init__(caso)

    def _trata_caso_retry(self, log: Logger):
        if self.caso.numero_tentativas >= MonitorNEWAVE.MAX_RETRY:
            raise ValueError(f"Caso {self.caso.nome} com falha" +
                             f"após {MonitorNEWAVE.MAX_RETRY}" +
                             " tentativas")
        log.info("Reagendando job para o caso:" +
                    f" {self.caso._nome_caso}")
        self.caso.coloca_caso_na_fila()
        self._gerenciador.agenda_job()

    def _trata_caso_executando(self,
                               log: Logger,
                               iniciou: bool) -> bool:
        retry = False
        iniciou = False
        if (self.caso.tempo_execucao >
            MonitorNEWAVE.TIMEOUT_COMUNICACAO):
            s = self._gerenciador.deleta_job()
            if not s:
                raise ValueError("Erro ao deletar o job " +
                                    f"{self.caso.nome}")
            retry = True
        if not iniciou:
            iniciou = True
            self.caso.inicia_caso()
            log.info(f"Iniciando execução do caso: {}")
        return retry, iniciou

    def executa_caso(self, log: Logger) -> bool:
        try:
            # Inicia a execução do job
            log.info(f"Agendando job para o caso: {self.caso.nome}")
            self.caso.inicializa_paramatros_execucao()
            self.caso.coloca_caso_na_fila()
            self._gerenciador.agenda_job()
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
                elif estado == EstadoJob.ERRO:
                    pass
                time.sleep(MonitorNEWAVE.INTERVALO_POLL)
        except TimeoutError as e:
            log.error(f"Timeout na execução do job {self.caso.nome}: {e}")
            return False
        except ValueError as e:
            log.error(f"Erro na execução do job {self.caso.nome}: {e}")
            return False