import time
from typing import Callable, Dict

from encadeador.services.unitofwork.job import factory as job_uow_factory
from encadeador.services.unitofwork.caso import factory as caso_uow_factory
from encadeador.services.unitofwork.estudo import factory as estudo_uow_factory

from encadeador.controladores.leitorarquivos import LeitorArquivos
from encadeador.controladores.monitorestudo import MonitorEstudo
from encadeador.modelos.transicaoestudo import TransicaoEstudo
from encadeador.utils.log import Log


# TODO - Aqui pode ser o lugar para ocorrer DI no futuro
# Uma maneira prática de fazer DI nessa versão é editar
# os singletons. Se precisar de multithreading, tem que pensar
# mais.. mas tem outras coisas que vão precisar mudar também.

UOW_KIND = "FS"
INTERVALO_POLL = 5.0
ESTUDO_ID = 1


class App:
    def __init__(self) -> None:
        self._lista_casos = LeitorArquivos.carrega_lista_casos()
        self._regras_reservatorio = (
            LeitorArquivos.carrega_regras_reservatorios()
        )
        self._regras_inviabilidades = (
            LeitorArquivos.carrega_regras_inviabilidades()
        )
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
                TransicaoEstudo.INICIO_EXECUCAO_SUCESSO
            ): self._handler_inicio_execucao_sucesso,
            (TransicaoEstudo.CONCLUIDO): self._handler_concluido,
            (TransicaoEstudo.ERRO): self._handler_erro,
        }

    def _handler_prepara_execucao_sucesso(self):
        self._monitor.inicia()

    def _handler_inicio_execucao_sucesso(self):
        self._executando = True

    def _handler_concluido(self):
        self.__finaliza(0)

    def _handler_erro(self):
        self.__finaliza(1)

    def __finaliza(self, codigo: int):
        Log.log().info("Finalizando Encadeador")
        exit(codigo)

    def inicializa(self):
        self._monitor = MonitorEstudo(
            ESTUDO_ID,
            estudo_uow_factory(UOW_KIND),
            caso_uow_factory(UOW_KIND),
            job_uow_factory(UOW_KIND),
            self._lista_casos,
            self._regras_reservatorio,
            self._regras_inviabilidades,
        )
        self._monitor.observa(self.callback_evento)
        self._monitor.prepara()

    def executa(self):
        while True:
            time.sleep(INTERVALO_POLL)
            Log.log().debug("Tentando monitorar...")
            if not self._executando:
                continue
            Log.log().debug("Monitorando...")
            self._monitor.monitora()
