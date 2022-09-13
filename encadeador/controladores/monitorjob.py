from typing import Callable, Tuple, Dict

from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.transicaojob import TransicaoJob
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.utils.log import Log
from encadeador.utils.event import Event

import encadeador.domain.commands as commands
import encadeador.services.handlers.job as handlers
from encadeador.services.unitofwork.job import AbstractJobUnitOfWork


# TODO - seria interessante ter uma AbstractMonitorJob ? Poderia
# esclarecer algumas coisas na hora de pensar na generalização
# para execução de casos em servidores diferentes, ou usando
# meios diferentes (arquivos em disco, buckets s3, etc..)


class MonitorJob:
    """
    Responsável por monitorar a execução
    de um job em um gerenciador de filas.
    Implementa o State Pattern para coordenar a execução do job,
    adquirindo informações do estado por meio do Observer Pattern.
    """

    def __init__(self, uow: AbstractJobUnitOfWork):
        self._job_id = None
        self._uow = uow
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

    def callback_estado_job(
        self, estado_atual: EstadoJob, novo_estado: EstadoJob
    ):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que o estado de um Job foi alterado
        em um GerenciadorFila e deve reagir atualizando os campos
        adequados nos objetos.

        :param novo_estado: O estado para o qual o job foi alterado.
        :type novo_estado: EstadoJob
        """
        # Executa a ação da transição de estado
        self._regras()[estado_atual, novo_estado]()

    def inicializa(self):
        self._transicao_job(TransicaoJob.CRIADO)

    def submete(
        self,
        caminho: str,
        nome: str,
        numero_processadores: int,
        id_caso: int,
        gerenciador: str = Configuracoes().gerenciador_fila,
    ) -> bool:
        comando = commands.SubmeteJob(
            gerenciador, caminho, nome, numero_processadores, id_caso
        )
        job = handlers.submete(comando, self._uow)

        self._transicao_job(TransicaoJob.SUBMISSAO_SOLICITADA)
        if job is None:
            self._transicao_job(TransicaoJob.SUBMISSAO_ERRO)
        else:
            self._job_id = job.id
        return job is not None

    def deleta(
        self,
        gerenciador: str = Configuracoes().gerenciador_fila,
    ) -> bool:
        comando = commands.DeletaJob(gerenciador, self._job_id)
        return handlers.deleta(comando, self._uow)

    def monitora(
        self,
        gerenciador: str = Configuracoes().gerenciador_fila,
    ):
        comando = commands.MonitoraJob(gerenciador, self._job_id)
        handlers.monitora(comando, self._uow, self.callback_estado_job)

    def observa(self, f: Callable):
        self._transicao_job.append(f)

    def _handler_submissao_sucesso(self):
        Log.log().info("Job - inserido na fila")
        self._transicao_job(TransicaoJob.SUBMISSAO_SUCESSO)

    def _handler_delecao_solicitada(self):
        Log.log().info("Job - solicitada deleção")
        self._transicao_job(TransicaoJob.DELECAO_SOLICITADA)

    def _handler_delecao_sucesso(self):
        Log.log().info("Job - deletado")
        self._transicao_job(TransicaoJob.DELECAO_SUCESSO)

    def _handler_inicio_execucao(self):
        Log.log().info("Job - início da execução")
        self._transicao_job(TransicaoJob.INICIO_EXECUCAO)

    def _handler_inicio_execucao_direto(self):
        self._handler_submissao_sucesso()
        self._handler_inicio_execucao()

    def _handler_fim_execucao(self):
        Log.log().info("Job - finalizado")
        self._transicao_job(TransicaoJob.FIM_EXECUCAO)

    def _handler_delecao_erro(self):
        Log.log().info("Job - erro de deleção")
        self._transicao_job(TransicaoJob.DELECAO_ERRO)

    def _handler_timeout_execucao(self):
        Log.log().info("Job - timeout")
        self._transicao_job(TransicaoJob.TIMEOUT_EXECUCAO)
