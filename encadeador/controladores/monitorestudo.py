from abc import abstractmethod
from typing import Dict, Tuple, Callable

from encadeador.modelos.caso import Caso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estudo import Estudo
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.controladores.armazenadorestudo import ArmazenadorEstudo
from encadeador.controladores.sintetizadorestudo import SintetizadorEstudo
from encadeador.utils.log import Log


class MonitorEstudo:
    """
    Responsável por monitorar a execução
    de um estudo através da execução dos seus casos.
    Implementa o State Pattern para coordenar a execução do estudo,
    adquirindo informações dos casos por meio do Observer Pattern.
    """

    def __init__(self, estudo: Estudo):
        self._estudo = estudo
        self._armazenador = ArmazenadorEstudo(estudo)
        self._caso_atual: Caso = None  # type: ignore
        self._monitor_atual: MonitorCaso = None  # type: ignore

    def callback_evento_caso(self, evento: TransicaoCaso):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que ocorreu algo com o caso
        e deve reagir atualizando os campos
        adequados nos objetos.

        :param evento: O evento ocorrido com o caso
        :type evento: TransicaoCaso
        """
        Log.log().info(f"Chamou callback evento Caso: {evento}")
        # Executa a ação da transição de estado
        novo_estado = self._regras()[self._estudo.estado, evento]()
        # Atualiza o estado atual
        self._estudo.atualiza(novo_estado, True)
        if not self._armazenador.armazena_estudo():
            Log.log().error("Erro ao armazenar estudo encadeado")

    @property
    @abstractmethod
    def caminho_job(self) -> str:
        pass

    @property
    @abstractmethod
    def nome_job(self) -> str:
        pass

    def _regras(
        self,
    ) -> Dict[Tuple[EstadoEstudo, TransicaoCaso], Callable[[], EstadoEstudo]]:
        return {
            (
                EstadoEstudo.EXECUTANDO,
                TransicaoCaso.INICIOU,
            ): self._trata_inicio_caso,
            (
                EstadoEstudo.EXECUTANDO,
                TransicaoCaso.SUCESSO,
            ): self._trata_fim_sucesso_caso,
            (
                EstadoEstudo.EXECUTANDO,
                TransicaoCaso.ERRO,
            ): self._trata_erro_caso,
            (
                EstadoEstudo.EXECUTANDO,
                TransicaoCaso.ERRO_MAX_FLEX,
            ): self._trata_erro_caso,
            (
                EstadoEstudo.EXECUTANDO,
                TransicaoCaso.ERRO_DADOS,
            ): self._trata_erro_caso,
        }

    def inicializa(self) -> bool:
        """
        Realiza a inicialização do caso, isto é, a preparação dos
        arquivos para adequação às necessidades do estudo
        encadeado e o encadeamento das variáveis selecionadas.

        :return: O sucesso ou não da inicialização do caso.
        :rtype: bool
        """
        self._caso_atual = self._estudo.proximo_caso
        self._monitor_atual = MonitorCaso.factory(self._caso_atual)
        self._monitor_atual.observa(self.callback_evento_caso)
        if not self._monitor_atual.inicializa(self._estudo.casos_concluidos):
            Log.log().error(
                "Erro de inicialização do caso " + f"{self._caso_atual.nome}"
            )
            return False
        if not self._monitor_atual.submete():
            Log.log().error(
                "Erro de submissão do caso " + f"{self._caso_atual.nome}"
            )
            return False
        return True

    def monitora(self):
        """
        Realiza o monitoramento do estado do estudo e também do
        caso atual em execução.
        """
        self._monitor_atual.monitora()
        if not self._armazenador.armazena_estudo():
            Log.log().error("Erro no armazenamento do estudo")
            raise RuntimeError()

    def _trata_inicio_caso(self) -> EstadoEstudo:
        sintetizador = SintetizadorEstudo(self._estudo)
        sintetizador.sintetiza_proximo_caso(self._estudo.proximo_caso)
        return EstadoEstudo.EXECUTANDO

    def _trata_fim_sucesso_caso(self) -> EstadoEstudo:
        sintetizador = SintetizadorEstudo(self._estudo)
        if not sintetizador.sintetiza_estudo():
            Log.log().error("Erro na síntese do estudo encadeado")
            raise RuntimeError()
        if self._estudo.terminou:
            return EstadoEstudo.CONCLUIDO
        else:
            if self._estudo.proximo_caso is None:
                Log.log().error("Não foi encontrado o próximo caso")
                raise RuntimeError()
            if not self.inicializa():
                raise RuntimeError()
            return EstadoEstudo.EXECUTANDO

    def _trata_erro_caso(self) -> EstadoEstudo:
        sintetizador = SintetizadorEstudo(self._estudo)
        if not sintetizador.sintetiza_estudo():
            Log.log().error("Erro na síntese do estudo encadeado")
            raise RuntimeError()
        return EstadoEstudo.ERRO
