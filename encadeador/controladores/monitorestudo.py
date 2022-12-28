from typing import Dict, List, Union, Callable
from os.path import join
from os import makedirs
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.modelos.regrainviabilidade import RegraInviabilidade
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.modelos.transicaoestudo import TransicaoEstudo
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.services.unitofwork.rodada import AbstractRodadaRepository
from encadeador.services.unitofwork.caso import AbstractCasoUnitOfWork
from encadeador.services.unitofwork.estudo import AbstractEstudoUnitOfWork
import encadeador.services.handlers.estudo as handlers

import encadeador.domain.commands as commands
from encadeador.adapters.repository.synthesis import (
    factory as synthesis_factory,
)
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
        rodada_uow: AbstractRodadaRepository,
        diretorios_casos: List[str],
        regras_reservatorios: List[RegraReservatorio],
        regras_inviabilidades: List[RegraInviabilidade],
    ):
        self._estudo_id = _estudo_id
        self._estudo_uow = estudo_uow
        self._caso_uow = caso_uow
        self._rodada_uow = rodada_uow
        self._diretorios_casos = diretorios_casos
        self._regras_reservatorios = regras_reservatorios
        self._regras_inviabilidades = regras_inviabilidades
        self._monitor_atual: MonitorCaso = None  # type: ignore
        self._transicao_estudo = Event()

    async def callback_evento(
        self, evento: Union[TransicaoCaso, TransicaoEstudo]
    ):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que ocorreu algo com o caso
        e deve reagir atualizando os campos
        adequados nos objetos.

        :param evento: O evento ocorrido com o caso ou o estudo
        :type evento: Union[TransicaoCaso, TransicaoEstudo]
        """
        await self._regras()[evento]()

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

    async def prepara(self):
        """
        Prepara a execução dos casos do estudo.
        """
        await self.callback_evento(TransicaoEstudo.PREPARA_EXECUCAO_SOLICITADA)

    async def inicia(self):
        """
        Inicia a execução dos casos do estudo.
        """
        await self.callback_evento(TransicaoEstudo.INICIO_EXECUCAO_SOLICITADA)

    def __existe_proximo_caso(self) -> bool:
        with self._estudo_uow:
            estudo = self._estudo_uow.estudos.read(self._estudo_id)
            return estudo.proximo_caso is not None

    async def __inicializa_proximo_caso(self):
        """
        Inicia a execução do proximo caso, isto é, a preparação dos
        arquivos para adequação às necessidades do estudo
        encadeado e o encadeamento das variáveis selecionadas.
        """
        with self._estudo_uow:
            estudo = self._estudo_uow.estudos.read(self._estudo_id)
            proximo_caso = estudo.proximo_caso
            nome = proximo_caso.nome
            id_caso = proximo_caso.id
        if proximo_caso is not None:
            await self.__sintetiza_estudo()
            Log.log().info(f"Estudo - Próximo caso: {nome}")
            self._monitor_atual = MonitorCaso(
                id_caso, self._caso_uow, self._rodada_uow
            )
            self._monitor_atual.observa(self.callback_evento)
            await self._monitor_atual.inicializa()

    async def monitora(self):
        """
        Realiza o monitoramento do estado do estudo e também do
        caso atual em execução.
        """
        Log.log().debug("Monitorando - estudo...")
        comando = commands.MonitoraEstudo(self._estudo_id)
        await handlers.monitora(comando, self._monitor_atual)

    async def _handler_prepara_execucao_solicitada(self):
        Log.log().info("Estudo: preparando execução")
        with self._estudo_uow:
            estudo = self._estudo_uow.estudos.read(self._estudo_id)
        if not estudo:
            comando_cria_estudo = commands.CriaEstudo(
                Configuracoes().caminho_base_estudo,
                Configuracoes().nome_estudo,
            )
            estudo = handlers.cria(comando_cria_estudo, self._estudo_uow)
            if not estudo:
                await self.callback_evento(
                    TransicaoEstudo.PREPARA_EXECUCAO_ERRO
                )

        comando_inicializa_estudo = commands.InicializaEstudo(
            self._estudo_id, self._diretorios_casos
        )
        handlers.inicializa(
            comando_inicializa_estudo, self._estudo_uow, self._caso_uow
        )
        await self.callback_evento(TransicaoEstudo.PREPARA_EXECUCAO_SUCESSO)

    async def _handler_prepara_execucao_sucesso(self):
        Log.log().info("Estudo: preparado com sucesso")
        await self.__sintetiza_estudo()
        await self._transicao_estudo(TransicaoEstudo.PREPARA_EXECUCAO_SUCESSO)

    async def _handler_prepara_execucao_erro(self):
        Log.log().info("Estudo: erro na preparação")
        await self.callback_evento(TransicaoEstudo.ERRO)

    async def _handler_inicio_execucao_solicitada(self):
        comando = commands.AtualizaEstudo(
            self._estudo_id, EstadoEstudo.INICIADO
        )
        if handlers.atualiza(comando, self._estudo_uow):
            Log.log().info("Iniciando Encadeador")
            await self.callback_evento(TransicaoEstudo.INICIO_EXECUCAO_SUCESSO)
        else:
            await self.callback_evento(TransicaoEstudo.INICIO_EXECUCAO_ERRO)

    async def _handler_inicio_execucao_sucesso(self):
        Log.log().info("Estudo: iniciando execução")
        comando = commands.AtualizaEstudo(
            self._estudo_id, EstadoEstudo.EXECUTANDO
        )
        if handlers.atualiza(comando, self._estudo_uow):
            await self._transicao_estudo(
                TransicaoEstudo.INICIO_EXECUCAO_SUCESSO
            )
            await self.callback_evento(TransicaoEstudo.INICIO_PROXIMO_CASO)

    async def _handler_inicio_execucao_erro(self):
        Log.log().info("Estudo: erro no início da execução")
        await self.callback_evento(TransicaoEstudo.ERRO)

    async def _handler_concluido(self):
        Log.log().info("Estudo: concluído.")
        comando = commands.AtualizaEstudo(
            self._estudo_id, EstadoEstudo.CONCLUIDO
        )
        handlers.atualiza(comando, self._estudo_uow)
        command = commands.SintetizaEstudo(self._estudo_id)
        await handlers.sintetiza_resultados(command, self._estudo_uow)
        await self._transicao_estudo(TransicaoEstudo.CONCLUIDO)

    async def _handler_erro(self):
        Log.log().info("Estudo: erro.")
        comando = commands.AtualizaEstudo(self._estudo_id, EstadoEstudo.ERRO)
        handlers.atualiza(comando, self._estudo_uow)
        await self.__sintetiza_estudo()
        await self._transicao_estudo(TransicaoEstudo.ERRO)

    async def _handler_inicializado_caso(self):
        Log.log().debug("Estudo: caso inicializado")
        await self._monitor_atual.prepara(self._regras_reservatorios)

    async def _handler_inicio_proximo_caso(self):
        if self.__existe_proximo_caso():
            await self.__inicializa_proximo_caso()
        else:
            await self.callback_evento(TransicaoEstudo.CONCLUIDO)

    async def _handler_prepara_execucao_solicitada_caso(self):
        Log.log().debug("Estudo: preparação da execução do caso solicitada")

    async def _handler_prepara_execucao_sucesso_caso(self):
        Log.log().debug(
            "Estudo: caso preparado com sucesso. Iniciando execução."
        )
        self._monitor_atual.inicia_execucao()

    async def _handler_inicio_execucao_solicitada_caso(self):
        Log.log().debug("Estudo: início da execução do caso solicitada")

    async def _handler_inicio_execucao_sucesso_caso(self):
        Log.log().info("Estudo: iniciando novo caso")

    async def _handler_concluido_caso(self):
        command = commands.SintetizaEstudo(self._estudo_id)
        await handlers.sintetiza_resultados(command, self._estudo_uow)
        await self.callback_evento(TransicaoEstudo.INICIO_PROXIMO_CASO)

    async def _handler_erro_caso(self):
        Log.log().error("Estudo: erro na execução do caso")
        await self.callback_evento(TransicaoEstudo.ERRO)

    async def __sintetiza_estudo(self):
        df_estudo = await handlers.sintetiza_estudo(self._estudo_uow)
        caminho_sintese = join(
            Configuracoes().caminho_base_estudo,
            Configuracoes().diretorio_sintese,
        )
        makedirs(caminho_sintese, exist_ok=True)
        sintetizador = synthesis_factory(Configuracoes().formato_sintese)
        sintetizador.write(df_estudo, join(caminho_sintese, "ESTUDO"))
