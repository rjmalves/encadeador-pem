from typing import Dict, List, Callable
from encadeador.services.unitofwork.rodada import AbstractRodadaRepository
from encadeador.services.unitofwork.caso import AbstractCasoUnitOfWork
from os.path import join
from os import makedirs
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.utils.log import Log
from encadeador.utils.event import Event
from encadeador.adapters.repository.synthesis import (
    factory as synthesis_factory,
)
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
        rodada_uow: AbstractRodadaRepository,
    ):
        self._caso_id = _caso_id
        self._rodada_id = None
        self._caso_uow = caso_uow
        self._rodada_uow = rodada_uow
        self._transicao_caso = Event()

    async def callback_evento(self, evento: TransicaoCaso):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que ocorreu algo com o job do caso
        em um GerenciadorFila ou algo de interesse aconteceu com o caso em si
        e o monitor deve reagir atualizando os campos adequados nos objetos.

        :param evento: O evento ocorrido com o job ou caso
        :type evento: Union[TransicaoCaso]
        """
        await self._regras()[evento]()

    def _regras(
        self,
    ) -> Dict[TransicaoCaso, Callable]:
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
        }

    async def inicializa(self):
        """
        Realiza a inicialização do caso, lidando com identificação e
        extração de arquivos.
        """
        comando = commands.InicializaCaso(self._caso_id)
        if (
            handlers.inicializa(comando, self._caso_uow, self._rodada_uow)
            is not None
        ):
            await self.callback_evento(TransicaoCaso.INICIALIZADO)

    async def prepara(
        self,
        regras_operacao_reservatorios: List[RegraReservatorio],
    ):
        """
        Realiza a preparação dos arquivos para adequação às
        necessidades do estudo encadeado e o encadeamento das
        variáveis selecionadas.
        """
        await self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA)
        comando = commands.PreparaCaso(
            self._caso_id, regras_operacao_reservatorios
        )
        if await handlers.prepara(comando, self._caso_uow):
            await self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_SUCESSO)
        else:
            await self.callback_evento(TransicaoCaso.PREPARA_EXECUCAO_ERRO)

    async def inicia_execucao(self):
        """
        Inicia o processo de execução de um caso após as etapas
        de inicialização e preparação.
        """
        await self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)

    async def __submete(self):
        """
        Cria um novo Job para o caso e o submete à fila.
        """
        comando = commands.SubmeteCaso(
            self._caso_id,
        )
        res = await handlers.submete(comando, self._caso_uow, self._rodada_uow)
        if isinstance(res, int):
            self._rodada_id = res
            await self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SUCESSO)
        else:
            await self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_ERRO)

    async def monitora(self):
        """
        Realiza o monitoramento do estado do caso e também do
        job associado.
        """
        if self._rodada_id is None:
            Log.log().info("Não existe rodada ativa para o caso")
            return
        comando = commands.MonitoraCaso(self._caso_id, self._rodada_id)
        transicao = await handlers.monitora(
            comando, self._caso_uow, self._rodada_uow
        )
        if transicao is not None:
            await self.callback_evento(transicao)

    def observa(self, f: Callable):
        self._transicao_caso.append(f)

    async def _handler_inicializado(self):
        Log.log().info(f"Caso {self._caso_id}: inicializado")
        await self._transicao_caso(TransicaoCaso.INICIALIZADO)

    async def _handler_prepara_execucao_solicitada(self):
        Log.log().info(f"Caso {self._caso_id}: iniciando preparação do caso")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.PREPARANDO)
        handlers.atualiza(comando, self._caso_uow)
        await self._transicao_caso(TransicaoCaso.PREPARA_EXECUCAO_SOLICITADA)

    async def _handler_prepara_execucao_sucesso(self):
        Log.log().info(f"Caso {self._caso_id}: caso preparado com sucesso")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.PREPARADO)
        handlers.atualiza(comando, self._caso_uow)
        await self.__sintetiza_casos_rodadas()
        await self._transicao_caso(TransicaoCaso.PREPARA_EXECUCAO_SUCESSO)

    async def _handler_prepara_execucao_erro(self):
        Log.log().info(f"Caso {self._caso_id}: erro na preparação do caso")
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_PREPARACAO
        )
        handlers.atualiza(comando, self._caso_uow)
        await self.callback_evento(TransicaoCaso.ERRO)

    async def _handler_inicio_execucao_solicitada(self):
        Log.log().info(f"Caso {self._caso_id}: solicitada execução do caso")
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.INICIANDO_EXECUCAO
        )
        handlers.atualiza(comando, self._caso_uow)
        await self._transicao_caso(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)
        await self.__submete()

    async def _handler_inicio_execucao_sucesso(self):
        Log.log().info(f"Caso {self._caso_id}: início da execução com sucesso")
        await self._transicao_caso(TransicaoCaso.INICIO_EXECUCAO_SUCESSO)
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.EXECUTANDO)
        handlers.atualiza(comando, self._caso_uow)
        await self.__sintetiza_casos_rodadas()
        # Nada a fazer, visto que agora existe o job na fila e as transições
        # acontecem escutando os eventos do Job, até ser finalizado.

    async def _handler_inicio_execucao_erro(self):
        Log.log().info(
            f"Caso {self._caso_id}: erro no início da execução do caso"
        )
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_EXECUCAO
        )
        handlers.atualiza(comando, self._caso_uow)
        await self.callback_evento(TransicaoCaso.ERRO)

    async def _handler_submissao_solicitada_job(self):
        Log.log().info(
            f"Caso {self._caso_id}: submissão do job do caso solicitada"
        )

    async def _handler_erro_dados(self):
        Log.log().info(f"Caso {self._caso_id}: erro de dados")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.ERRO_DADOS)
        handlers.atualiza(comando, self._caso_uow)
        await self.callback_evento(TransicaoCaso.ERRO)

    async def _handler_erro_convergencia(self):
        Log.log().info(f"Caso {self._caso_id}: erro na convergência")

        comando = commands.CorrigeErroConvergenciaCaso(self._caso_id)
        if await handlers.corrige_erro_convergencia(comando, self._caso_uow):
            await self.callback_evento(
                TransicaoCaso.INICIO_EXECUCAO_SOLICITADA
            )
        else:
            comando = commands.AtualizaCaso(
                self._caso_id, EstadoCaso.ERRO_EXECUCAO
            )
            handlers.atualiza(comando, self._caso_uow)
            await self.callback_evento(TransicaoCaso.ERRO)

    async def _handler_nao_convergiu(self):
        Log.log().info(f"Caso {self._caso_id}: não convergiu")
        comando = commands.FlexibilizaCriterioConvergenciaCaso(self._caso_id)
        if await handlers.flexibiliza_criterio_convergencia(
            comando, self._caso_uow
        ):
            await self.callback_evento(
                TransicaoCaso.INICIO_EXECUCAO_SOLICITADA
            )
        else:
            comando = commands.AtualizaCaso(
                self._caso_id, EstadoCaso.ERRO_PREPARACAO
            )
            handlers.atualiza(comando, self._caso_uow)
            await self.callback_evento(TransicaoCaso.ERRO)

    async def _handler_erro_max_flex(self):
        Log.log().info(
            f"Caso {self._caso_id}: máximo de flexibilizações atingido"
        )
        comando = commands.AtualizaCaso(
            self._caso_id, EstadoCaso.ERRO_MAX_FLEX
        )
        handlers.atualiza(comando, self._caso_uow)
        await self.callback_evento(TransicaoCaso.ERRO)

    async def _handler_caso_inviavel(self):
        Log.log().info(f"Caso {self._caso_id}: caso inviável")
        comando = commands.FlexibilizaCaso(
            self._caso_id, Configuracoes().maximo_flexibilizacoes_revisao
        )
        ret = await handlers.flexibiliza(comando, self._caso_uow)
        if ret is None:
            comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.ERRO)
            handlers.atualiza(comando, self._caso_uow)
            await self.callback_evento(TransicaoCaso.ERRO)
        else:
            await self.__sintetiza_casos_rodadas()
            await self.callback_evento(ret)

    async def _handler_flexibilizacao_sucesso(self):
        Log.log().info(
            f"Caso {self._caso_id}: flexibilização realizada com sucesso."
        )
        await self.callback_evento(TransicaoCaso.INICIO_EXECUCAO_SOLICITADA)

    async def _handler_flexibilizacao_erro(self):
        Log.log().info(f"Caso {self._caso_id}: erro na flexibilização.")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.ERRO)
        handlers.atualiza(comando, self._caso_uow)
        await self.callback_evento(TransicaoCaso.ERRO)

    async def _handler_caso_concluido(self):
        Log.log().info(f"Caso {self._caso_id}: caso concluído.")
        comando = commands.AtualizaCaso(self._caso_id, EstadoCaso.CONCLUIDO)
        handlers.atualiza(comando, self._caso_uow)
        await self.__sintetiza_casos_rodadas()
        await self._transicao_caso(TransicaoCaso.CONCLUIDO)

    async def _handler_erro(self):
        await self.__sintetiza_casos_rodadas()
        Log.log().error(f"Caso {self._caso_id}: Erro. ")
        self._transicao_caso(TransicaoCaso.ERRO)

    async def __sintetiza_casos_rodadas(self):
        df_casos, df_rodadas = await handlers.sintetiza_casos_rodadas(
            self._caso_uow, self._rodada_uow
        )
        caminho_sintese = join(
            Configuracoes().caminho_base_estudo,
            Configuracoes().diretorio_sintese,
        )
        makedirs(caminho_sintese, exist_ok=True)
        sintetizador = synthesis_factory(Configuracoes().formato_sintese)
        sintetizador.write(df_casos, join(caminho_sintese, "CASOS"))
        sintetizador.write(df_rodadas, join(caminho_sintese, "RODADAS"))
