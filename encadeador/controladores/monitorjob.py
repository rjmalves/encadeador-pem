from typing import Callable, Tuple, Dict

from encadeador.modelos.job import Job
from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.transicaojob import TransicaoJob
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.gerenciadorfila import GerenciadorFila
from encadeador.utils.log import Log
from encadeador.utils.event import Event


class MonitorJob:
    """
    Responsável por monitorar a execução
    de um job em um gerenciador de filas.
    Implementa o State Pattern para coordenar a execução do job,
    adquirindo informações do estado por meio do Observer Pattern.
    """

    TIMEOUT_ERRO_COMUNICACAO = 1800

    def __init__(self, job: Job):
        self._job = job
        g = Configuracoes().gerenciador_fila
        self._gerenciador = GerenciadorFila.factory(g)
        self._gerenciador.observa(self.callback_estado_job)
        self._transicao_job = Event()

    def _regras(self) -> Dict[Tuple[EstadoJob, EstadoJob], Callable]:
        return {
            (
                EstadoJob.NAO_INICIADO,
                EstadoJob.ESPERANDO,
            ): self._trata_entrada_fila,
            (
                EstadoJob.ESPERANDO,
                EstadoJob.DELETANDO,
            ): self._trata_comando_deleta_job,
            (
                EstadoJob.ESPERANDO,
                EstadoJob.EXECUTANDO,
            ): self._trata_inicio_execucao,
            (
                EstadoJob.EXECUTANDO,
                EstadoJob.DELETANDO,
            ): self._trata_comando_deleta_job,
            (
                EstadoJob.EXECUTANDO,
                EstadoJob.FINALIZADO,
            ): self._trata_fim_execucao,
            (EstadoJob.EXECUTANDO, EstadoJob.ERRO): self._trata_erro_execucao,
            (
                EstadoJob.DELETANDO,
                EstadoJob.FINALIZADO,
            ): self._trata_job_deletado,
        }

    def callback_estado_job(self, novo_estado: EstadoJob):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que o estado de um Job foi alterado
        em um GerenciadorFila e deve reagir atualizando os campos
        adequados nos objetos.

        :param novo_estado: O estado para o qual o job foi alterado.
        :type novo_estado: EstadoJob
        """
        # Executa a ação da transição de estado
        self._regras()[self._job.estado, novo_estado]()
        # Atualiza o estado atual
        self._job.atualiza(novo_estado)

    def submete(self, numero_processadores: int) -> bool:
        r = self._gerenciador.agenda_job(
            self._job.caminho, self._job.nome, numero_processadores
        )
        self._job.id = self._gerenciador.id_job
        self._job.numero_processadores = numero_processadores
        return r

    def deleta(self):
        if not self._gerenciador.deleta_job():
            Log.log().error(
                "Erro ao executar comando de deleção "
                + f"do job {self._job.id}[{self._job.nome}]"
            )
            raise RuntimeError()

    def monitora(self):
        self._gerenciador.monitora_estado_job()
        if all(
            [
                self._job.estado == EstadoJob.EXECUTANDO,
                self._gerenciador.tempo_job_idle
                > __class__.TIMEOUT_ERRO_COMUNICACAO,
            ]
        ):
            self.deleta()

    def observa(self, f: Callable):
        self._transicao_job.append(f)

    def _trata_entrada_fila(self):
        Log.log().info(f"Job {self._job.id}[{self._job.nome}] inserido na fila")
        self._transicao_job(TransicaoJob.ENTRADA_FILA)

    def _trata_comando_deleta_job(self):
        Log.log().info(
            f"Job {self._job.id}[{self._job.nome}] - solicitada deleção"
        )
        self._transicao_job(TransicaoJob.COMANDO_DELETA_JOB)

    def _trata_job_deletado(self):
        Log.log().info(f"Job {self._job.id}[{self._job.nome}] - deletado")
        self._transicao_job(TransicaoJob.JOB_DELETADO)

    def _trata_inicio_execucao(self):
        Log.log().info(
            f"Job {self._job.id}[{self._job.nome}] - início da execução"
        )
        self._transicao_job(TransicaoJob.INICIO_EXECUCAO)

    def _trata_fim_execucao(self):
        Log.log().info(f"Job {self._job.id}[{self._job.nome}] - finalizado")
        self._transicao_job(TransicaoJob.FIM_EXECUCAO)

    def _trata_erro_delecao(self):
        Log.log().info(
            f"Job {self._job.id}[{self._job.nome}] - erro de deleção"
        )
        self._transicao_job(TransicaoJob.ERRO_DELECAO)

    def _trata_erro_execucao(self):
        Log.log().info(
            f"Job {self._job.id}[{self._job.nome}] - erro de execução"
        )
        self._transicao_job(TransicaoJob.ERRO_EXECUCAO)
