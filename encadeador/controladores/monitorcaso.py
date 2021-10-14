from abc import abstractmethod
from typing import Tuple
from logging import Logger
from os.path import join
from os import listdir
import time
from encadeador.controladores.avaliadorcaso import AvaliadorCaso

from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.modelos.estadojob import EstadoJob
from encadeador.controladores.gerenciadorfila import GerenciadorFila
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorCaso


class MonitorCaso:
    """
    Responsável por executar e monitorar a execução
    de um caso em um gerenciador de filas.
    """
    INTERVALO_POLL = 5.0
    MAX_RETRY = 3
    TIMEOUT_COMUNICACAO = 1800.0

    def __init__(self,
                 caso: Caso,
                 log: Logger):
        self._caso = caso
        self._log = log
        g = caso.configuracoes.gerenciador_fila
        self._gerenciador = GerenciadorFila.factory(g)
        self._armazenador = ArmazenadorCaso(caso, log)
        self._avaliador = AvaliadorCaso.factory(caso, log)

    @staticmethod
    def factory(caso: Caso,
                log: Logger) -> 'MonitorCaso':
        if isinstance(caso, CasoNEWAVE):
            return MonitorNEWAVE(caso, log)
        elif isinstance(caso, CasoDECOMP):
            return MonitorDECOMP(caso, log)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    @property
    @abstractmethod
    def caminho_job(self) -> str:
        pass

    @property
    @abstractmethod
    def nome_job(self) -> str:
        pass

    def _trata_caso_retry(self):
        if self.caso.numero_tentativas >= self.__class__.MAX_RETRY:
            raise ValueError(f"Caso {self.caso.nome} com falha" +
                             f"após {self.__class__.MAX_RETRY}" +
                             " tentativas")
        self._log.info("Reagendando job para o caso:" +
                       f" {self.caso.nome}")
        self.caso.coloca_caso_na_fila()
        self._gerenciador.agenda_job(self.caminho_job,
                                     self.nome_job,
                                     self.caso.numero_processadores)

    def _trata_caso_executando(self,
                               iniciado: bool) -> Tuple[bool, bool]:
        retry = False
        iniciou = iniciado
        if not iniciado:
            iniciou = True
            self.caso.inicia_caso()
            self._log.info(f"Iniciando execução do caso: {self.caso.nome}")
        elif (self._gerenciador.tempo_job_idle >
              self.__class__.TIMEOUT_COMUNICACAO):
            self._log.info(f"Erro de comunicacao no caso: {self.caso.nome}.")
            s = self._gerenciador.deleta_job()
            if not s:
                raise ValueError("Erro ao deletar o job " +
                                 f"{self.caso.nome}")
            retry = True
        return retry, iniciou

    def _trata_caso_erro(self) -> bool:
        self._log.error(f"Erro na execução do caso: {self.caso.nome}")
        s = self._gerenciador.deleta_job()
        if not s:
            raise ValueError("Erro ao deletar o job " +
                             f"{self.caso.nome}")
        return True

    def executa_caso(self) -> bool:
        try:
            # Inicia a execução do job
            self._log.info(f"Agendando job para o caso: {self.caso.nome}")
            self.caso.inicializa_parametros_execucao()
            self.caso.coloca_caso_na_fila()
            self._gerenciador.agenda_job(self.caminho_job,
                                         self.nome_job,
                                         self.caso.numero_processadores)
            ultimo_estado = EstadoJob.NAO_INICIADO
            retry = False
            iniciou = False
            while True:
                # Execução rápida do retry, se precisar
                if retry:
                    self._trata_caso_retry()
                    retry = False
                    iniciou = False
                # Máquina de estados para controlar a execução
                estado = self._gerenciador.estado_job
                if estado == EstadoJob.NAO_INICIADO:
                    if ultimo_estado == EstadoJob.EXECUTANDO:
                        sucesso = self._avaliador.avalia()
                        self.caso.finaliza_caso(sucesso)
                        self._log.info("Finalizada execução do caso + "
                                       f"{self.caso.nome}")
                        return True
                    else:
                        self._log.error("Erro na execução do caso" +
                                        f" {self.caso.nome}")
                        return False
                elif estado == EstadoJob.ESPERANDO:
                    pass
                elif estado == EstadoJob.EXECUTANDO:
                    retry, iniciou = self._trata_caso_executando(iniciou)
                elif estado == EstadoJob.DELETANDO:
                    pass
                elif estado == EstadoJob.ERRO:
                    retry = self._trata_caso_erro()
                ultimo_estado = estado
                if not self._armazenador.armazena_caso():
                    raise ValueError(f"Erro ao armazenar {self.caso.nome}")
                time.sleep(self.__class__.INTERVALO_POLL)
        except TimeoutError as e:
            self._log.error(f"Timeout na execução do caso {self.caso.nome}: {e}")
            return False
        except ValueError as e:
            self._log.error(f"Erro na execução do caso {self.caso.nome}: {e}")
            return False

    @property
    def caso(self) -> Caso:
        return self._caso


class MonitorNEWAVE(MonitorCaso):
    INTERVALO_POLL = 5.0
    MAX_RETRY = 3
    TIMEOUT_COMUNICACAO = 1800.0

    def __init__(self,
                 caso: CasoNEWAVE,
                 log: Logger):
        super().__init__(caso, log)

    @property
    def caso(self) -> CasoNEWAVE:
        if not isinstance(self._caso, CasoNEWAVE):
            raise ValueError("MonitorNEWAVE tem um caso não de NEWAVE")
        return self._caso

    # Override
    def _obtem_caminho_job(self) -> str:
        cfg = self.caso.configuracoes
        dir_base = cfg.diretorio_instalacao_newaves
        versao = cfg.versao_newave
        dir_versao = join(dir_base, versao)
        arquivos_versao = listdir(dir_versao)
        arq_job = [a for a in arquivos_versao if ".job" in a]
        return join(dir_versao, arq_job[0])

    # Override
    def _obtem_nome_job(self) -> str:
        return f"NW{self.caso.ano}{self.caso.mes}"


class MonitorDECOMP(MonitorCaso):
    INTERVALO_POLL = 5.0
    MAX_RETRY = 3
    TIMEOUT_COMUNICACAO = 300.0

    def __init__(self, caso: CasoDECOMP, log: Logger):
        super().__init__(caso, log)

    @property
    def caso(self) -> CasoDECOMP:
        if not isinstance(self._caso, CasoDECOMP):
            raise ValueError("MonitorDECOMP tem um caso não de DECOMP")
        return self._caso

    # Override
    def _obtem_caminho_job(self) -> str:
        cfg = self.caso.configuracoes
        dir_base = cfg.diretorio_instalacao_decomps
        versao = cfg.versao_decomp
        dir_versao = join(dir_base, versao)
        arquivos_versao = listdir(dir_versao)
        arq_job = [a for a in arquivos_versao if ".job" in a]
        return join(dir_versao, arq_job[0])

    # Override
    def _obtem_nome_job(self) -> str:
        return f"DC{self.caso.ano}{self.caso.mes}{self.caso.revisao}"
