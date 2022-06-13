from os import chdir
import time
from typing import Callable, Dict

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.monitorestudo import MonitorEstudo
from encadeador.modelos.transicaoestudo import TransicaoEstudo
from encadeador.utils.log import Log

INTERVALO_POLL = 5.0


# TODO - Aqui pode ser o lugar para ocorrer DI no futuro
# Uma maneira prática de fazer DI nessa versão é editar
# os singletons. Se precisar de multithreading, tem que pensar
# mais.. mas tem outras coisas que vão precisar mudar também.


class App:
    def __init__(self) -> None:
        self._monitor = MonitorEstudo()
        self._monitor.observa(self.callback_evento)
        self._executando = False

    def callback_evento(self, evento: TransicaoEstudo):
        """
        Esta função é usada para implementar o Observer Pattern.
        Quando chamada, significa que ocorreu algo com o estudo.

        :param evento: O evento ocorrido com o estudo
        :type evento: TransicaoEstudo
        """
        regras = self._regras()
        if evento in regras.keys():
            regras[evento]()
        else:
            Log.log().warning(f"Evento não capturado: {evento.name}")

    def _regras(
        self,
    ) -> Dict[TransicaoEstudo, Callable]:
        return {
            (
                TransicaoEstudo.PREPARA_EXECUCAO_SUCESSO
            ): self._handler_prepara_execucao_sucesso,
            (
                TransicaoEstudo.INICIO_EXECUCAO_SOLICITADA
            ): self._handler_inicio_execucao_solicitada,
            (
                TransicaoEstudo.INICIO_EXECUCAO_SUCESSO
            ): self._handler_inicio_execucao_sucesso,
            (TransicaoEstudo.CONCLUIDO): self._handler_concluido,
            (TransicaoEstudo.ERRO): self._handler_erro,
        }

    def _handler_prepara_execucao_sucesso(self):
        self._monitor.inicia()

    def _handler_inicio_execucao_solicitada(self):
        Log.log().info(f"Iniciando Encadeador - {Configuracoes().nome_estudo}")

    def _handler_inicio_execucao_sucesso(self):
        self._executando = True

    def _handler_concluido(self):
        self.__finaliza(0)

    def _handler_erro(self):
        self.__finaliza(1)

    def __finaliza(self, codigo: int):
        Log.log().info("Finalizando Encadeador")
        exit(codigo)

    def executa(self):
        self._monitor.prepara()
        while True:
            chdir(Configuracoes().caminho_base_estudo)
            time.sleep(INTERVALO_POLL)
            if not self._executando:
                continue
            Log.log().info("Monitorando...")
            self._monitor.monitora()
