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
        # Executa a ação da transição de estado
        self._regras()[self._estudo.estado, evento]()

        if not self._armazenador.armazena_estudo():
            Log.log().error("Erro ao armazenar estudo encadeado")
        sintetizador = SintetizadorEstudo(self._estudo)
        if not sintetizador.sintetiza_estudo():
            Log.log().error("Erro na síntese do estudo encadeado")
            raise RuntimeError()

    def _regras(
        self,
    ) -> Dict[Tuple[EstadoEstudo, TransicaoCaso], Callable]:
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
        caso = self._estudo.proximo_caso
        if caso is None:
            if self._estudo.terminou:
                self._estudo.atualiza(EstadoEstudo.CONCLUIDO)
                Log.log().info("Estudo já finalizado")
                return True
            else:
                self._estudo.atualiza(EstadoEstudo.ERRO)
                Log.log().error("Não foi encontrado o próximo caso")
                return False
        self._caso_atual = caso
        Log.log().info(
            f"Estudo {self._estudo.nome} - Próximo caso: {caso.nome}"
        )
        self._estudo.atualiza(EstadoEstudo.EXECUTANDO)
        self._monitor_atual = MonitorCaso.factory(self._caso_atual)
        self._monitor_atual.observa(self.callback_evento_caso)
        if not self._monitor_atual.inicializa(
            self._estudo.casos_concluidos, self._estudo._regras_reservatorio
        ):
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

    def _trata_inicio_caso(self):
        sintetizador = SintetizadorEstudo(self._estudo)
        sintetizador.sintetiza_proximo_caso(self._estudo.proximo_caso)
        Log.log().info(f"Estudo {self._estudo.nome} - Iniciando novo caso")
        self._estudo.atualiza(EstadoEstudo.EXECUTANDO)

    def _trata_fim_sucesso_caso(self):
        if self._estudo.terminou:
            Log.log().info(f"Estudo: {self._estudo.nome} - Estudo concluído")
            self._estudo.atualiza(EstadoEstudo.CONCLUIDO)
        else:
            if self._estudo.proximo_caso is None:
                Log.log().error("Não foi encontrado o próximo caso")
                self._estudo.atualiza(EstadoEstudo.ERRO)
                raise RuntimeError()
            if not self.inicializa():
                self._estudo.atualiza(EstadoEstudo.ERRO)
                raise RuntimeError()
            Log.log().info(
                f"Estudo: {self._estudo.nome} - Caso concluído com sucesso"
            )
            self._estudo.atualiza(EstadoEstudo.EXECUTANDO)

    def _trata_erro_caso(self):
        Log.log().error(
            f"Estudo: {self._estudo.nome} - Erro na execução de caso"
        )
        self._estudo.atualiza(EstadoEstudo.ERRO)
