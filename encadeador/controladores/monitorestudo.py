from typing import Dict, Union, Callable
from encadeador.controladores.preparadorestudo import PreparadorEstudo

from encadeador.modelos.caso import Caso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estudo import Estudo
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.modelos.transicaoestudo import TransicaoEstudo
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.controladores.armazenadorestudo import ArmazenadorEstudo
from encadeador.controladores.sintetizadorestudo import SintetizadorEstudo
from encadeador.utils.log import Log
from encadeador.utils.event import Event


class MonitorEstudo:
    """
    Responsável por monitorar a execução
    de um estudo através da execução dos seus casos.
    Implementa o State Pattern para coordenar a execução do estudo,
    adquirindo informações dos casos por meio do Observer Pattern.
    """

    def __init__(self):
        self._estudo: Estudo = None  # type: ignore
        self._armazenador: ArmazenadorEstudo = None  # type: ignore
        self._sintetizador: SintetizadorEstudo = None  # type: ignore
        self._caso_atual: Caso = None  # type: ignore
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
            (TransicaoCaso.CRIADO): self._handler_criado_caso,
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
        Realiza a preparação do estudo encadeado para a sua execução. Caso
        O estudo ainda não tenha sido iniciado, os arquivos para armazenamento
        das saídas são criados, a conferência da existência dos casos a
        serem executados é feita, etc.
        """
        self._estudo = ArmazenadorEstudo.gera_estudo()
        self._armazenador = ArmazenadorEstudo(self._estudo)
        self._sintetizador = SintetizadorEstudo(self._estudo)
        self.callback_evento(TransicaoEstudo.PREPARA_EXECUCAO_SOLICITADA)
        preparador = PreparadorEstudo(self._estudo)
        if preparador.prepara_estudo():
            self.callback_evento(TransicaoEstudo.PREPARA_EXECUCAO_SUCESSO)
        else:
            self.callback_evento(TransicaoEstudo.PREPARA_EXECUCAO_ERRO)

    def inicia(self):
        """
        Inicia a execução dos casos do estudo.
        """
        self.callback_evento(TransicaoEstudo.INICIO_EXECUCAO_SOLICITADA)

    def __existe_proximo_caso(self) -> bool:
        return self._estudo.proximo_caso is not None

    def __inicializa_proximo_caso(self):
        """
        Inicia a execução do proximo caso, isto é, a preparação dos
        arquivos para adequação às necessidades do estudo
        encadeado e o encadeamento das variáveis selecionadas.
        """
        self._caso_atual = self._estudo.proximo_caso
        Log.log().info(
            f"Estudo {self._estudo.nome} - Próximo: {self._caso_atual.nome}"
        )
        self._monitor_atual = MonitorCaso.factory(self._caso_atual)
        self._monitor_atual.observa(self.callback_evento)
        self._monitor_atual.inicializa(self._estudo.casos_concluidos)

    def monitora(self):
        """
        Realiza o monitoramento do estado do estudo e também do
        caso atual em execução.
        """
        Log.log().debug("Monitorando - estudo...")
        self._monitor_atual.monitora()
        if not self._armazenador.armazena_estudo():
            Log.log().error("Erro no armazenamento do estudo")
            raise RuntimeError()

    def _handler_prepara_execucao_solicitada(self):
        Log.log().info(
            f"Estudo {self._estudo.nome}: iniciando preparação do estudo"
        )

    def _handler_prepara_execucao_sucesso(self):
        Log.log().info(
            f"Estudo {self._estudo.nome}: estudo preparado com sucesso"
        )
        self._estudo.estado = EstadoEstudo.NAO_INICIADO
        self._transicao_estudo(TransicaoEstudo.PREPARA_EXECUCAO_SUCESSO)

    def _handler_prepara_execucao_erro(self):
        Log.log().info(
            f"Estudo {self._estudo.nome}: erro na preparação do estudo"
        )
        self.__armazena_estudo()
        self.__sintetiza_estudo()
        self.callback_evento(TransicaoEstudo.ERRO)

    def _handler_inicio_execucao_solicitada(self):
        self._transicao_estudo(TransicaoEstudo.INICIO_EXECUCAO_SOLICITADA)
        self._estudo.estado = EstadoEstudo.INICIADO
        self.callback_evento(TransicaoEstudo.INICIO_EXECUCAO_SUCESSO)

    def _handler_inicio_execucao_sucesso(self):
        Log.log().info(
            f"Estudo {self._estudo.nome}: iniciando execução do estudo"
        )
        self._transicao_estudo(TransicaoEstudo.INICIO_EXECUCAO_SUCESSO)
        self._estudo.estado = EstadoEstudo.EXECUTANDO
        self.callback_evento(TransicaoEstudo.INICIO_PROXIMO_CASO)

    def _handler_inicio_execucao_erro(self):
        Log.log().info(
            f"Estudo {self._estudo.nome}: erro no início da execução"
            + " do estudo."
        )
        self.callback_evento(TransicaoEstudo.ERRO)

    def _handler_concluido(self):
        Log.log().info(f"Estudo {self._estudo.nome}: concluído.")
        self._estudo.estado = EstadoEstudo.CONCLUIDO
        self.__armazena_estudo()
        self.__sintetiza_estudo()
        self._transicao_estudo(TransicaoEstudo.CONCLUIDO)

    def _handler_erro(self):
        Log.log().info(f"Estudo {self._estudo.nome}: erro.")
        self._estudo.estado = EstadoEstudo.ERRO
        self._transicao_estudo(TransicaoEstudo.ERRO)

    def _handler_criado_caso(self):
        Log.log().debug(f"Estudo {self._estudo.nome}: caso criado")
        self._monitor_atual.prepara(
            self._estudo.casos_concluidos, self._estudo._regras_reservatorio
        )

    def _handler_inicio_proximo_caso(self):
        if self.__existe_proximo_caso():
            self.__inicializa_proximo_caso()
        else:
            self.callback_evento(TransicaoEstudo.CONCLUIDO)

    def _handler_prepara_execucao_solicitada_caso(self):
        Log.log().debug(
            f"Estudo {self._estudo.nome}: preparação da execução"
            + " do caso solicitada"
        )

    def _handler_prepara_execucao_sucesso_caso(self):
        Log.log().debug(
            f"Estudo {self._estudo.nome}: caso preparado com sucesso."
            + " Iniciando execução."
        )
        self._monitor_atual.inicia_execucao()

    def _handler_inicio_execucao_solicitada_caso(self):
        Log.log().debug(
            f"Estudo {self._estudo.nome}: início da execução"
            + " do caso solicitada"
        )

    def _handler_inicio_execucao_sucesso_caso(self):
        sintetizador = SintetizadorEstudo(self._estudo)
        sintetizador.sintetiza_proximo_caso(self._estudo.proximo_caso)
        Log.log().info(f"Estudo {self._estudo.nome} - iniciando novo caso")

    def _handler_concluido_caso(self):
        self.__armazena_estudo()
        self.__sintetiza_estudo()
        self.callback_evento(TransicaoEstudo.INICIO_PROXIMO_CASO)

    def _handler_erro_caso(self):
        Log.log().error(
            f"Estudo: {self._estudo.nome} - erro na execução do caso"
        )
        self.__armazena_estudo()
        self.__sintetiza_estudo()
        self.callback_evento(TransicaoEstudo.ERRO)

    def __armazena_estudo(self):
        if not self._armazenador.armazena_estudo():
            Log.log().error(
                f"Estudo {self._estudo.nome}: Erro no armazenamento do estudo"
                + f" - estado: {self._estudo.estado}."
            )
            self.callback_evento(TransicaoEstudo.ERRO)

    def __sintetiza_estudo(self):
        if not self._sintetizador.sintetiza_estudo():
            Log.log().error(
                f"Estudo {self._estudo.nome}: Erro na síntese do estudo - "
                + f"estado: {self._estudo.estado}."
            )
            self.callback_evento(TransicaoEstudo.ERRO)

    @property
    def estudo(self) -> Estudo:
        return self._estudo
