from typing import Dict, List, Union, Callable
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.modelos.regrainviabilidade import RegraInviabilidade
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.modelos.transicaoestudo import TransicaoEstudo
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.services.unitofwork.job import AbstractJobUnitOfWork
from encadeador.services.unitofwork.caso import AbstractCasoUnitOfWork
from encadeador.services.unitofwork.estudo import AbstractEstudoUnitOfWork
import encadeador.services.handlers.estudo as handlers
import encadeador.domain.commands as commands

from encadeador.utils.log import Log
from encadeador.utils.event import Event


class MonitorEstudo:
    """
    Responsável por monitorar a execução
    de um estudo através da execução dos seus casos.
    Implementa o State Pattern para coordenar a execução do estudo,
    adquirindo informações dos casos por meio do Observer Pattern.
    """

    def __init__(
        self,
        _estudo_id: int,
        estudo_uow: AbstractEstudoUnitOfWork,
        caso_uow: AbstractCasoUnitOfWork,
        job_uow: AbstractJobUnitOfWork,
        diretorios_casos: List[str],
        regras_reservatorios: List[RegraReservatorio],
        regras_inviabilidades: List[RegraInviabilidade],
    ):
        self._estudo_id = _estudo_id
        self._estudo_uow = estudo_uow
        self._caso_uow = caso_uow
        self._job_uow = job_uow
        self._diretorios_casos = diretorios_casos
        self._regras_reservatorios = regras_reservatorios
        self._regras_inviabilidades = regras_inviabilidades
        self._monitor_atual: MonitorCaso = None  # type: ignore
        self._transicao_estudo = Event()

    def callback_evento(self, evento: Union[TransicaoCaso, TransicaoEstudo]):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que ocorreu algo com o caso
        e deve reagir atualizando os campos
        adequados nos objetos.

        :param evento: O evento ocorrido com o caso ou o estudo
        :type evento: Union[TransicaoCaso, TransicaoEstudo]
        """
        self._regras()[evento]()

    def observa(self, f: Callable):
        self._transicao_estudo.append(f)

    def _regras(
        self,
    ) -> Dict[Union[TransicaoEstudo, TransicaoCaso], Callable]:
        return {
            (
                TransicaoEstudo.PREPARA_EXECUCAO_SOLICITADA
            ): self._handler_prepara_execucao_solicitada,
            (
                TransicaoEstudo.PREPARA_EXECUCAO_SUCESSO
            ): self._handler_prepara_execucao_sucesso,
            (
                TransicaoEstudo.PREPARA_EXECUCAO_ERRO
            ): self._handler_prepara_execucao_erro,
            (
                TransicaoEstudo.INICIO_EXECUCAO_SOLICITADA
            ): self._handler_inicio_execucao_solicitada,
            (
                TransicaoEstudo.INICIO_EXECUCAO_SUCESSO
            ): self._handler_inicio_execucao_sucesso,
            (
                TransicaoEstudo.INICIO_EXECUCAO_ERRO
            ): self._handler_inicio_execucao_erro,
            (
                TransicaoEstudo.INICIO_PROXIMO_CASO
            ): self._handler_inicio_proximo_caso,
            (TransicaoEstudo.CONCLUIDO): self._handler_concluido,
            (TransicaoEstudo.ERRO): self._handler_erro,
            (TransicaoCaso.INICIALIZADO): self._handler_inicializado_caso,
            (
                TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA
            ): self._handler_prepara_execucao_solicitada_caso,
            (
                TransicaoCaso.PREPARA_EXECUCAO_SUCESSO
            ): self._handler_prepara_execucao_sucesso_caso,
            (
                TransicaoCaso.INICIO_EXECUCAO_SOLICITADA
            ): self._handler_inicio_execucao_solicitada_caso,
            (
                TransicaoCaso.INICIO_EXECUCAO_SUCESSO
            ): self._handler_inicio_execucao_sucesso_caso,
            (TransicaoCaso.CONCLUIDO): self._handler_concluido_caso,
            (TransicaoCaso.ERRO): self._handler_erro_caso,
        }

    def prepara(self):
        """
        Prepara a execução dos casos do estudo.
        """
        self.callback_evento(TransicaoEstudo.PREPARA_EXECUCAO_SOLICITADA)

    def inicia(self):
        """
        Inicia a execução dos casos do estudo.
        """
        self.callback_evento(TransicaoEstudo.INICIO_EXECUCAO_SOLICITADA)

    def __existe_proximo_caso(self) -> bool:
        with self._estudo_uow:
            estudo = self._estudo_uow.estudos.read(self._estudo_id)
            return estudo.proximo_caso is not None

    def __inicializa_proximo_caso(self):
        """
        Inicia a execução do proximo caso, isto é, a preparação dos
        arquivos para adequação às necessidades do estudo
        encadeado e o encadeamento das variáveis selecionadas.
        """
        with self._estudo_uow:
            estudo = self._estudo_uow.estudos.read(self._estudo_id)
            caso_atual = estudo.proximo_caso
        if caso_atual is not None:
            Log.log().info(f"Estudo - Próximo caso: {caso_atual.nome}")
            self._monitor_atual = MonitorCaso(
                caso_atual.id, self._caso_uow, self._job_uow
            )
            self._monitor_atual.observa(self.callback_evento)
            self._monitor_atual.inicializa()

    def monitora(self):
        """
        Realiza o monitoramento do estado do estudo e também do
        caso atual em execução.
        """
        Log.log().debug("Monitorando - estudo...")
        comando = commands.MonitoraEstudo(self._estudo_id)
        handlers.monitora(comando, self._monitor_atual)

    def _handler_prepara_execucao_solicitada(self):
        Log.log().info("Estudo: preparando execução")
        with self._estudo_uow:
            estudo = self._estudo_uow.estudos.read(self._estudo_id)
        if not estudo:
            comando_cria_estudo = commands.CriaEstudo(
                Configuracoes.caminho_base_estudo,
                Configuracoes.nome_estudo,
            )
            estudo = handlers.cria(comando_cria_estudo, self._estudo_uow)
            if not estudo:
                self.callback_evento(TransicaoEstudo.PREPARA_EXECUCAO_ERRO)
        else:
            comando_inicializa_estudo = commands.InicializaEstudo(
                estudo.id, self._diretorios_casos
            )
            handlers.inicializa(
                comando_inicializa_estudo, self._estudo_uow, self._caso_uow
            )
        self.callback_evento(TransicaoEstudo.PREPARA_EXECUCAO_SUCESSO)

    def _handler_prepara_execucao_sucesso(self):
        Log.log().info("Estudo: preparado com sucesso")
        self._transicao_estudo(TransicaoEstudo.PREPARA_EXECUCAO_SUCESSO)

    def _handler_prepara_execucao_erro(self):
        Log.log().info("Estudo: erro na preparação")
        self.callback_evento(TransicaoEstudo.ERRO)

    def _handler_inicio_execucao_solicitada(self):
        comando = commands.AtualizaEstudo(
            self._estudo_id, EstadoEstudo.INICIADO
        )
        if handlers.atualiza(comando, self._estudo_uow):
            Log.log().info("Iniciando Encadeador")
            self.callback_evento(TransicaoEstudo.INICIO_EXECUCAO_SUCESSO)
        else:
            self.callback_evento(TransicaoEstudo.INICIO_EXECUCAO_ERRO)

    def _handler_inicio_execucao_sucesso(self):
        Log.log().info("Estudo: iniciando execução")
        comando = commands.AtualizaEstudo(
            self._estudo_id, EstadoEstudo.EXECUTANDO
        )
        if handlers.atualiza(comando, self._estudo_uow):
            self._transicao_estudo(TransicaoEstudo.INICIO_EXECUCAO_SUCESSO)
            self.callback_evento(TransicaoEstudo.INICIO_PROXIMO_CASO)

    def _handler_inicio_execucao_erro(self):
        Log.log().info("Estudo: erro no início da execução")
        self.callback_evento(TransicaoEstudo.ERRO)

    def _handler_concluido(self):
        Log.log().info("Estudo: concluído.")
        comando = commands.AtualizaEstudo(
            self._estudo_id, EstadoEstudo.CONCLUIDO
        )
        handlers.atualiza(comando, self._estudo_uow)
        self._transicao_estudo(TransicaoEstudo.CONCLUIDO)

    def _handler_erro(self):
        Log.log().info("Estudo: erro.")
        comando = commands.AtualizaEstudo(self._estudo_id, EstadoEstudo.ERRO)
        handlers.atualiza(comando, self._estudo_uow)
        self._transicao_estudo(TransicaoEstudo.ERRO)

    def _handler_inicializado_caso(self):
        Log.log().debug("Estudo: caso inicializado")
        self._monitor_atual.prepara(self._regras_reservatorios)

    def _handler_inicio_proximo_caso(self):
        if self.__existe_proximo_caso():
            self.__inicializa_proximo_caso()
        else:
            self.callback_evento(TransicaoEstudo.CONCLUIDO)

    def _handler_prepara_execucao_solicitada_caso(self):
        Log.log().debug("Estudo: preparação da execução do caso solicitada")

    def _handler_prepara_execucao_sucesso_caso(self):
        Log.log().debug(
            "Estudo: caso preparado com sucesso. Iniciando execução."
        )
        self._monitor_atual.inicia_execucao()

    def _handler_inicio_execucao_solicitada_caso(self):
        Log.log().debug("Estudo: início da execução do caso solicitada")

    def _handler_inicio_execucao_sucesso_caso(self):
        Log.log().info("Estudo: iniciando novo caso")

    def _handler_concluido_caso(self):
        comando = commands.SintetizaEstudo(self._estudo_id)
        handlers.sintetiza(comando, self._estudo_uow)
        self.callback_evento(TransicaoEstudo.INICIO_PROXIMO_CASO)

    def _handler_erro_caso(self):
        Log.log().error("Estudo: erro na execução do caso")
        self.callback_evento(TransicaoEstudo.ERRO)
