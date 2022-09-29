from typing import Dict, List, Callable, Union
from encadeador.controladores.monitorjob import MonitorJob
from encadeador.services.unitofwork.job import AbstractJobUnitOfWork
from encadeador.services.unitofwork.caso import AbstractCasoUnitOfWork

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.modelos.transicaojob import TransicaoJob
from encadeador.utils.log import Log
from encadeador.utils.event import Event
import encadeador.domain.commands as commands
import encadeador.services.handlers.caso as handlers


class MonitorCaso:
    """
    Responsável por monitorar a execução
    de um caso através dos seus jobs.
    Implementa o State Pattern para coordenar a execução do caso,
    adquirindo informações do estado dos jobs por meio do Observer Pattern.
    """

    def __init__(
        self,
        _caso_id: int,
        caso_uow: AbstractCasoUnitOfWork,
        job_uow: AbstractJobUnitOfWork,
    ):
        self._caso_id = _caso_id
        self._caso_uow = caso_uow
        self._job_uow = job_uow
        self._monitor_job_atual: MonitorJob = None  # type: ignore
        self._transicao_caso = Event()

    def callback_evento(self, evento: Union[TransicaoJob, TransicaoCaso]):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que ocorreu algo com o job do caso
        em um GerenciadorFila ou algo de interesse aconteceu com o caso em si
        e o monitor deve reagir atualizando os campos adequados nos objetos.

        :param evento: O evento ocorrido com o job ou caso
        :type evento: Union[TransicaoJob, TransicaoCaso]
        """
        self._regras()[evento]()

    def _regras(
        self,
    ) -> Dict[Union[TransicaoJob, TransicaoCaso], Callable]:
        return {
            TransicaoCaso.INICIALIZADO: self._handler_inicializado,
            (
                TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA
            ): self._handler_prepara_execucao_solicitada,
            (
                TransicaoCaso.PREPARA_EXECUCAO_SUCESSO
            ): self._handler_prepara_execucao_sucesso,
            (
                TransicaoCaso.PREPARA_EXECUCAO_ERRO
            ): self._handler_prepara_execucao_erro,
            (
                TransicaoCaso.INICIO_EXECUCAO_SOLICITADA
            ): self._handler_inicio_execucao_solicitada,
            (
                TransicaoCaso.INICIO_EXECUCAO_SUCESSO
            ): self._handler_inicio_execucao_sucesso,
            (
                TransicaoCaso.INICIO_EXECUCAO_ERRO
            ): self._handler_inicio_execucao_erro,
            (TransicaoCaso.ERRO_DADOS): self._handler_erro_dados,
            (TransicaoCaso.ERRO_CONVERGENCIA): self._handler_erro_convergencia,
            (TransicaoCaso.NAO_CONVERGIU): self._handler_nao_convergiu,
            (TransicaoCaso.ERRO_MAX_FLEX): self._handler_erro_max_flex,
            (
                TransicaoCaso.FLEXIBILIZACAO_SUCESSO
            ): self._handler_flexibilizacao_sucesso,
            (
                TransicaoCaso.FLEXIBILIZACAO_ERRO
            ): self._handler_flexibilizacao_erro,
            (TransicaoCaso.CONCLUIDO): self._handler_caso_concluido,
            (TransicaoCaso.INVIAVEL): self._handler_caso_inviavel,
            (TransicaoCaso.ERRO): self._handler_erro,
            (
                TransicaoJob.SUBMISSAO_SOLICITADA
            ): self._handler_submissao_solicitada_job,
            (
                TransicaoJob.SUBMISSAO_SUCESSO
            ): self._handler_submissao_sucesso_job,
            (TransicaoJob.SUBMISSAO_ERRO): self._handler_submissao_erro_job,
            (TransicaoJob.INICIO_EXECUCAO): self._handler_inicio_execucao_job,
            (TransicaoJob.FIM_EXECUCAO): self._handler_fim_execucao_job,
            (TransicaoJob.TIMEOUT_EXECUCAO): self._handler_timeout_execucao,
            (
                TransicaoJob.DELECAO_SOLICITADA
            ): self._handler_delecao_solicitada,
            (TransicaoJob.DELECAO_ERRO): self._handler_delecao_erro,
            (TransicaoJob.DELECAO_SUCESSO): self._handler_delecao_sucesso,
        }

    def inicializa(self):
        """
        Realiza a inicialização do caso, lidando com identificação e
        extração de arquivos.
        """
        comando = commands.InicializaCaso(self._caso_id)
        if handlers.inicializa(comando, self._caso_uow) is not None:
            self.callback_evento(TransicaoCaso.INICIALIZADO)

    def prepara(
        self,
        regras_operacao_reservatorios: List[RegraReservatorio],
    ):
        """
        Realiza a preparação dos arquivos para adequação às
        necessidades do estudo encadeado e o encadeamento das
        variáveis selecionadas.
        """
        self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA)
        comando = commands.PreparaCaso(
            self._caso_id, regras_operacao_reservatorios
        )
        if handlers.prepara(comando, self._caso_uow):
            self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_SUCESSO)
        else:
            self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_ERRO)

    def inicia_execucao(self):
        """
        Inicia o processo de execução de um caso após as etapas
        de inicialização e preparação.
        """
        self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)

    def __submete(self):
        """
        Cria um novo Job para o caso e o submete à fila.
        """
        comando = commands.SubmeteCaso(
            self._caso_id,
            Configuracoes().gerenciador_fila,
        )
        self._monitor_job_atual = MonitorJob(self._job_uow)
        self._monitor_job_atual.observa(self.callback_evento)
        if handlers.submete(comando, self._caso_uow, self._monitor_job_atual):
            self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SUCESSO)
        else:
            self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_ERRO)

    def monitora(self):
        """
        Realiza o monitoramento do estado do caso e também do
        job associado.
        """
        comando = commands.MonitoraCaso(
            self._caso_id, Configuracoes().gerenciador_fila
        )
        handlers.monitora(comando, self._caso_uow, self._monitor_job_atual)

    def observa(self, f: Callable):
        self._transicao_caso.append(f)

    def _handler_inicializado(self):
        Log.log().info(f"Caso {self._caso_id}: inicializado")
        self._transicao_caso(TransicaoCaso.INICIALIZADO)

    def _handler_prepara_execucao_solicitada(self):
        Log.log().info(f"Caso {self._caso_id}: iniciando preparação do caso")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.PREPARANDO)
        handlers.atualiza(comando, self._caso_uow)
        self._transicao_caso(TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA)

    def _handler_prepara_execucao_sucesso(self):
        Log.log().info(f"Caso {self._caso_id}: caso preparado com sucesso")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.PREPARADO)
        handlers.atualiza(comando, self._caso_uow)
        self._transicao_caso(TransicaoCaso.PREPARA_EXECUCAO_SUCESSO)

    def _handler_prepara_execucao_erro(self):
        Log.log().info(f"Caso {self._caso_id}: erro na preparação do caso")
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_PREPARACAO
        )
        handlers.atualiza(comando, self._caso_uow)
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_inicio_execucao_solicitada(self):
        Log.log().info(f"Caso {self._caso_id}: solicitada execução do caso")
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.INICIANDO_EXECUCAO
        )
        handlers.atualiza(comando, self._caso_uow)
        self._transicao_caso(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)
        self.__submete()

    def _handler_inicio_execucao_sucesso(self):
        Log.log().info(f"Caso {self._caso_id}: início da execução com sucesso")
        self._transicao_caso(TransicaoCaso.INICIO_EXECUCAO_SUCESSO)
        # Nada a fazer, visto que agora existe o job na fila e as transições
        # acontecem escutando os eventos do Job, até ser finalizado.

    def _handler_inicio_execucao_erro(self):
        Log.log().info(
            f"Caso {self._caso_id}: erro no início da execução do caso"
        )
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_EXECUCAO
        )
        handlers.atualiza(comando, self._caso_uow)
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_submissao_solicitada_job(self):
        Log.log().info(
            f"Caso {self._caso_id}: submissão do job do caso solicitada"
        )

    def _handler_submissao_sucesso_job(self):
        Log.log().info(f"Caso {self._caso_id}: sucesso na submissão do caso")
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ESPERANDO_FILA
        )
        handlers.atualiza(comando, self._caso_uow)

    def _handler_submissao_erro_job(self):
        Log.log().info(
            f"Caso {self._caso_id}: erro na submissão do job do caso"
        )
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_COMUNICACAO
        )
        handlers.atualiza(comando, self._caso_uow)
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_inicio_execucao_job(self):
        Log.log().info(f"Caso {self._caso_id}: iniciou execução")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.EXECUTANDO)
        handlers.atualiza(comando, self._caso_uow)

    def _handler_delecao_solicitada(self):
        # Aguarda o job ser deletado completamente
        Log.log().debug(f"Caso {self._caso_id}: deleção solicitada")

    def _handler_delecao_erro(self):
        Log.log().info(
            f"Caso {self._caso_id}: erro na deleção de um caso com timeout"
        )
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_COMUNICACAO
        )
        handlers.atualiza(comando, self._caso_uow)
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_delecao_sucesso(self):
        Log.log().debug(f"Caso {self._caso_id}: caso com timeout deletado")
        self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)

    def _handler_erro_dados(self):
        Log.log().info(f"Caso {self._caso_id}: erro de dados")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.ERRO_DADOS)
        handlers.atualiza(comando, self._caso_uow)
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_erro_convergencia(self):
        Log.log().info(f"Caso {self._caso_id}: erro na convergência")

        comando = commands.CorrigeErroConvergenciaCaso(self._caso_id)
        if handlers.corrige_erro_convergencia(comando, self._caso_uow):
            self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)
        else:
            comando = commands.AtualizaCaso(
                self._caso_id, EstadoCaso.ERRO_EXECUCAO
            )
            handlers.atualiza(comando, self._caso_uow)
            self.callback_evento(TransicaoCaso.ERRO)

    def _handler_nao_convergiu(self):
        Log.log().info(f"Caso {self._caso_id}: não convergiu")
        comando = commands.FlexibilizaCriterioConvergenciaCaso(self._caso_id)
        if handlers.flexibiliza_criterio_convergencia(comando, self._caso_uow):
            self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)
        else:
            comando = commands.AtualizaCaso(
                self._caso_id, EstadoCaso.ERRO_PREPARACAO
            )
            handlers.atualiza(comando, self._caso_uow)
            self.callback_evento(TransicaoCaso.ERRO)

    def _handler_erro_max_flex(self):
        Log.log().info(
            f"Caso {self._caso_id}: máximo de flexibilizações atingido"
        )
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_MAX_FLEX
        )
        handlers.atualiza(comando, self._caso_uow)
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_timeout_execucao(self):
        Log.log().debug(f"Caso {self._caso_id}: timeout durante a execução")
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_COMUNICACAO
        )
        handlers.atualiza(comando, self._caso_uow)
        self._monitor_job_atual.deleta()

    def _handler_fim_execucao_job(self):
        Log.log().info(f"Caso {self._caso_id}: fim da execução")
        comando = commands.AvaliaCaso(self._caso_id)
        ret = handlers.avalia(comando, self._caso_uow)
        if ret is None:
            comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.ERRO)
            handlers.atualiza(comando, self._caso_uow)
            self.callback_evento(TransicaoCaso.ERRO)
        else:
            self.callback_evento(ret)

    def _handler_caso_inviavel(self):
        Log.log().info(f"Caso {self._caso_id}: caso inviável")
        comando = commands.FlexibilizaCaso(
            self._caso_id, Configuracoes().maximo_flexibilizacoes_revisao
        )
        comando_sintese = commands.SintetizaCaso(self._caso_id, "execucao")
        handlers.sintetiza(comando_sintese, self._caso_uow)
        ret = handlers.flexibiliza(comando, self._caso_uow)
        if ret is None:
            comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.ERRO)
            handlers.atualiza(comando, self._caso_uow)
            self.callback_evento(TransicaoCaso.ERRO)
        else:
            self.callback_evento(ret)

    def _handler_flexibilizacao_sucesso(self):
        Log.log().info(
            f"Caso {self._caso_id}: flexibilização realizada com sucesso."
        )
        self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)

    def _handler_flexibilizacao_erro(self):
        Log.log().info(f"Caso {self._caso_id}: erro na flexibilização.")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.ERRO)
        handlers.atualiza(comando, self._caso_uow)
        self.callback_evento(TransicaoCaso.ERRO)

    def _handler_caso_concluido(self):
        Log.log().info(f"Caso {self._caso_id}: caso concluído.")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.CONCLUIDO)
        handlers.atualiza(comando, self._caso_uow)
        comando_sintese = commands.SintetizaCaso(self._caso_id, "completa")
        handlers.sintetiza(comando_sintese, self._caso_uow)
        self._transicao_caso(TransicaoCaso.CONCLUIDO)

    def _handler_erro(self):
        Log.log().error(f"Caso {self._caso_id}: Erro. ")
