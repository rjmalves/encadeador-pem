from os import chdir
import time

from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estudo import Estudo
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.monitorestudo import MonitorEstudo
from encadeador.controladores.armazenadorestudo import ArmazenadorEstudo
from encadeador.controladores.sintetizadorestudo import SintetizadorEstudo
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.utils.log import Log

INTERVALO_POLL = 5.0


class App:
    def __init__(self) -> None:
        self._estudo: Estudo = None  # type: ignore
        self._monitor: MonitorEstudo = None  # type: ignore

    def __constroi_estudo_encadeado(self):
        self._estudo = ArmazenadorEstudo.gera_estudo()
        # Força as regras a serem sempre as lidas do CSV
        # TODO - melhorar essa lógica (organização dos dados em DB)
        caminho_regras = Configuracoes().arquivo_regras_operacao_reservatorios
        if caminho_regras is not None:
            self._estudo._regras_reservatorio = RegraReservatorio.from_csv(
                caminho_regras
            )
        else:
            self._estudo._regras_reservatorio = []
        self._monitor = MonitorEstudo(self._estudo)

    def executa(self) -> bool:
        Log.log().info(f"Iniciando Encadeador - {Configuracoes().nome_estudo}")
        try:
            self.__constroi_estudo_encadeado()
            # Refaz a síntese como estão as coisas
            if not self._monitor.inicializa():
                sintetizador = SintetizadorEstudo(self._estudo)
                sintetizador.sintetiza_estudo()
                raise RuntimeError()
            # O programa fica nesse loop até acabar o estudo, ou ocorrer erro
            while not self._estudo.terminou:
                chdir(Configuracoes().caminho_base_estudo)
                self._monitor.monitora()
                time.sleep(INTERVALO_POLL)
            Log.log().info("Finalizando Encadeador")
            return self._estudo.estado == EstadoEstudo.CONCLUIDO
        except Exception as e:
            Log.log().error(f"Execução do Encadeador interrompida: {e}")
            raise e
