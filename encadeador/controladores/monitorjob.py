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
            ): self._handler_submissao_sucesso,
            (
                EstadoJob.ESPERANDO,
                EstadoJob.DELETANDO,
            ): self._handler_delecao_solicitada,
            (
                EstadoJob.NAO_INICIADO,
                EstadoJob.EXECUTANDO,
            ): self._handler_inicio_execucao_direto,
            (
                EstadoJob.ESPERANDO,
                EstadoJob.EXECUTANDO,
            ): self._handler_inicio_execucao,
            (
                EstadoJob.EXECUTANDO,
                EstadoJob.DELETANDO,
            ): self._handler_delecao_erro,
            (
                EstadoJob.EXECUTANDO,
                EstadoJob.FINALIZADO,
            ): self._handler_fim_execucao,
            (
                EstadoJob.DELETANDO,
                EstadoJob.FINALIZADO,
            ): self._handler_delecao_sucesso,
            (
                EstadoJob.EXECUTANDO,
                EstadoJob.TIMEOUT,
            ): self._handler_timeout_execucao,
            (
                EstadoJob.TIMEOUT,
                EstadoJob.DELETANDO,
            ): self._handler_delecao_solicitada,
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
        estado_atual = self._job.estado
        # Atualiza o estado atual
        self._job.atualiza(novo_estado)
        # Executa a ação da transição de estado
        self._regras()[estado_atual, novo_estado]()

    def inicializa(self):
        self._transicao_job(TransicaoJob.CRIADO)

    def submete(self, numero_processadores: int) -> bool:
        r = self._gerenciador.agenda_job(
            self._job.caminho, self._job.nome, numero_processadores
        )
        self._job.id = self._gerenciador.id_job
        self._job.numero_processadores = numero_processadores
        self._transicao_job(TransicaoJob.SUBMISSAO_SOLICITADA)
        if not r:
            self._transicao_job(TransicaoJob.SUBMISSAO_ERRO)
        return r

    def deleta(self) -> bool:
        return self._gerenciador.deleta_job()

    def monitora(self):
        Log.log().info("Monitorando - job...")
        self._gerenciador.monitora_estado_job()

    def observa(self, f: Callable):
        self._transicao_job.append(f)

    def _handler_submissao_sucesso(self):
        Log.log().info(
            f"Job {self._job.id}[{self._job.nome}] inserido na fila"
        )
        self._transicao_job(TransicaoJob.SUBMISSAO_SUCESSO)

    def _handler_delecao_solicitada(self):
        Log.log().info(
            f"Job {self._job.id}[{self._job.nome}] - solicitada deleção"
        )
        self._transicao_job(TransicaoJob.DELECAO_SOLICITADA)

    def _handler_delecao_sucesso(self):
        Log.log().info(f"Job {self._job.id}[{self._job.nome}] - deletado")
        self._transicao_job(TransicaoJob.DELECAO_SUCESSO)

    def _handler_inicio_execucao(self):
        Log.log().info(
            f"Job {self._job.id}[{self._job.nome}] - início da execução"
        )
        self._transicao_job(TransicaoJob.INICIO_EXECUCAO)

    def _handler_inicio_execucao_direto(self):
        self._handler_submissao_sucesso()
        self._handler_inicio_execucao()

    def _handler_fim_execucao(self):
        Log.log().info(f"Job {self._job.id}[{self._job.nome}] - finalizado")
        self._transicao_job(TransicaoJob.FIM_EXECUCAO)

    def _handler_delecao_erro(self):
        Log.log().info(
            f"Job {self._job.id}[{self._job.nome}] - erro de deleção"
        )
        self._transicao_job(TransicaoJob.DELECAO_ERRO)

    def _handler_timeout_execucao(self):
        Log.log().info(f"Job {self._job.id}[{self._job.nome}] - timeout")
        self._transicao_job(TransicaoJob.TIMEOUT_EXECUCAO)
