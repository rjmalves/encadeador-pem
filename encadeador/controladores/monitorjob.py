from typing import Callable, Tuple, Dict
import time

from encadeador.modelos.job import Job
from encadeador.modelos.caso import Caso
from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.gerenciadorfila import GerenciadorFila
from encadeador.utils.log import Log


class MonitorJob:
    """
    Responsável por executar e monitorar a execução
    de um job em um gerenciador de filas.
    Implementa o State Pattern para coordenar a execução do job,
    adquirindo informações do estado por meio do Observer Pattern.
    """
    INTERVALO_POLL = 5.0
    MAX_RETRY = 3

    def __init__(self,
                 job: Job,
                 caso: Caso):
        self._job = job
        g = Configuracoes().gerenciador_fila
        self._gerenciador = GerenciadorFila.factory(g)
        self._gerenciador.observa(self.monitor)

    def _regras(self) -> Dict[Tuple[EstadoJob,
                                    EstadoJob],
                              Callable]:
        return {
            (EstadoJob.NAO_INICIADO,
             EstadoJob.ESPERANDO): self._trata_entrada_fila,
            (EstadoJob.ESPERANDO,
             EstadoJob.DELETANDO): self._trata_comando_deleta_job,
            (EstadoJob.ESPERANDO,
             EstadoJob.EXECUTANDO): self._trata_inicio_execucao,
            (EstadoJob.EXECUTANDO,
             EstadoJob.DELETANDO): self._trata_comando_deleta_job,
            (EstadoJob.EXECUTANDO,
             EstadoJob.FINALIZADO): self._trata_fim_execucao,
            (EstadoJob.EXECUTANDO,
             EstadoJob.ERRO): self._trata_erro_execucao,
            (EstadoJob.DELETANDO,
             EstadoJob.FINALIZADO): self._trata_job_deletado
        }

    def monitor(self,
                novo_estado: EstadoJob):
        # Executa a ação da transição de estado
        self._regras()[self._job.estado, novo_estado]()
        # Atualiza o estado atual
        self._job.estado = novo_estado

    def submete(self,
                caminho_job: str,
                nome_job: str,
                numero_processadores: int):
        self._gerenciador.agenda_job(caminho_job,
                                     nome_job,
                                     numero_processadores)

    def _trata_entrada_fila(self):
        Log.log(f"Job {self._job.id}[{self._job.nome}] inserido na fila")
        pass

    def _trata_comando_deleta_job(self):
        Log.log(f"Job {self._job.id}[{self._job.nome}] - solicitada deleção")
        pass

    def _trata_job_deletado(self):
        Log.log(f"Job {self._job.id}[{self._job.nome}] - deletado")
        pass

    def _trata_inicio_execucao(self):
        Log.log(f"Job {self._job.id}[{self._job.nome}] - início da execução")
        pass

    def _trata_fim_execucao(self):
        Log.log(f"Job {self._job.id}[{self._job.nome}] - finalizado")
        pass

    def _trata_erro_delecao(self):
        Log.log(f"Job {self._job.id}[{self._job.nome}] - erro de deleção")
        pass

    def _trata_erro_execucao(self):
        Log.log(f"Job {self._job.id}[{self._job.nome}] - erro de execução")
        pass

    def _trata_caso_retry(self):
        if self.caso.numero_tentativas >= self.__class__.MAX_RETRY:
            raise ValueError(f"Caso {self.caso.nome} com falha" +
                             f"após {self.__class__.MAX_RETRY}" +
                             " tentativas")
        Log.log().info("Reagendando job para o caso:" +
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
            Log.log().info(f"Iniciando execução do caso: {self.caso.nome}")
        elif (self._gerenciador.tempo_job_idle >
              self.__class__.TIMEOUT_COMUNICACAO):
            Log.log().info(f"Erro de comunicacao no caso: {self.caso.nome}.")
            s = self._gerenciador.deleta_job()
            if not s:
                raise ValueError("Erro ao deletar o job " +
                                 f"{self.caso.nome}")
            retry = True
        return retry, iniciou

    def _trata_caso_erro(self) -> bool:
        Log.log().error(f"Erro na execução do caso: {self.caso.nome}")
        Log.log().info("Deletando job da fila...")
        s = self._gerenciador.deleta_job()
        if not s:
            raise ValueError("Erro ao deletar o job " +
                             f"{self.caso.nome}")
        return True

    def executa_caso(self) -> bool:
        try:
            # Inicia a execução do job
            Log.log().info(f"Agendando job para o caso: {self.caso.nome}")
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
                        Log.log().info("Finalizada execução do caso " +
                                       f"{self.caso.nome}")
                        return True
                    elif iniciou:
                        Log.log().error("Erro na execução do caso" +
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

                # Atualiza o último estado
                ultimo_estado = estado
                if not self._armazenador.armazena_caso():
                    raise ValueError(f"Erro ao armazenar {self.caso.nome}")
                time.sleep(self.__class__.INTERVALO_POLL)
        except TimeoutError:
            Log.log().error(f"Timeout na execução do caso {self.caso.nome}")
            return False
        except ValueError as e:
            Log.log().error(f"Erro na execução do caso {self.caso.nome}: {e}")
            return False

    @property
    def caso(self) -> Caso:
        return self._caso
