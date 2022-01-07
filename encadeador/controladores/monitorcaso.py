from abc import abstractmethod
from typing import Tuple
from os.path import join
from os import listdir
import time
from encadeador.controladores.avaliadorcaso import AvaliadorCaso

from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.estadojob import EstadoJob
from encadeador.controladores.gerenciadorfila import GerenciadorFila
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.utils.log import Log


class MonitorCaso:
    """
    Responsável por executar e monitorar a execução
    de um caso em um gerenciador de filas.
    """
    INTERVALO_POLL = 5.0
    MAX_RETRY = 3
    TIMEOUT_COMUNICACAO = 1800.0

    def __init__(self,
                 caso: Caso):
        self._caso = caso
        g = Configuracoes().gerenciador_fila
        self._gerenciador = GerenciadorFila.factory(g)
        self._armazenador = ArmazenadorCaso(caso)
        self._avaliador = AvaliadorCaso.factory(caso)

    @staticmethod
    def factory(caso: Caso) -> 'MonitorCaso':
        if isinstance(caso, CasoNEWAVE):
            return MonitorNEWAVE(caso)
        elif isinstance(caso, CasoDECOMP):
            return MonitorDECOMP(caso)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    @property
    @abstractmethod
    def caminho_job(self) -> str:
        pass

    @property
    @abstractmethod
    def nome_job(self) -> str:
        pass

    def _trata_caso_retry(self):
        if self.caso.numero_tentativas >= self.__class__.MAX_RETRY:
            raise ValueError(f"Caso {self.caso.nome} com falha" +
                             f"após {self.__class__.MAX_RETRY}" +
                             " tentativas")
        Log.log().info("Reagendando job para o caso:" +
                       f" {self.caso.nome}")
        self.caso.coloca_caso_na_fila()
        self._gerenciador.agenda_job(self.caminho_job,
                                     self.nome_job,
                                     self.caso.numero_processadores)

    def _trata_caso_executando(self,
                               iniciado: bool) -> Tuple[bool, bool]:
        retry = False
        iniciou = iniciado
        if not iniciado:
            iniciou = True
            self.caso.inicia_caso()
            Log.log().info(f"Iniciando execução do caso: {self.caso.nome}")
        elif (self._gerenciador.tempo_job_idle >
              self.__class__.TIMEOUT_COMUNICACAO):
            Log.log().info(f"Erro de comunicacao no caso: {self.caso.nome}.")
            s = self._gerenciador.deleta_job()
            if not s:
                raise ValueError("Erro ao deletar o job " +
                                 f"{self.caso.nome}")
            retry = True
        return retry, iniciou

    def _trata_caso_erro(self) -> bool:
        Log.log().error(f"Erro na execução do caso: {self.caso.nome}")
        Log.log().info("Deletando job da fila...")
        s = self._gerenciador.deleta_job()
        if not s:
            raise ValueError("Erro ao deletar o job " +
                             f"{self.caso.nome}")
        return True

    def executa_caso(self) -> bool:
        try:
            # Inicia a execução do job
            Log.log().info(f"Agendando job para o caso: {self.caso.nome}")
            self.caso.inicializa_parametros_execucao()
            self.caso.coloca_caso_na_fila()
            self._gerenciador.agenda_job(self.caminho_job,
                                         self.nome_job,
                                         self.caso.numero_processadores)
            ultimo_estado = EstadoJob.NAO_INICIADO
            retry = False
            iniciou = False
            while True:
                # Execução rápida do retry, se precisar
                if retry:
                    self._trata_caso_retry()
                    retry = False
                    iniciou = False
                # Máquina de estados para controlar a execução
                estado = self._gerenciador.estado_job
                if estado == EstadoJob.NAO_INICIADO:
                    if ultimo_estado == EstadoJob.EXECUTANDO:
                        sucesso = self._avaliador.avalia()
                        self.caso.finaliza_caso(sucesso)
                        Log.log().info("Finalizada execução do caso " +
                                       f"{self.caso.nome}")
                        return True
                    elif iniciou:
                        Log.log().error("Erro na execução do caso" +
                                        f" {self.caso.nome}")
                        return False
                elif estado == EstadoJob.ESPERANDO:
                    pass
                elif estado == EstadoJob.EXECUTANDO:
                    retry, iniciou = self._trata_caso_executando(iniciou)
                elif estado == EstadoJob.DELETANDO:
                    pass
                elif estado == EstadoJob.ERRO:
                    retry = self._trata_caso_erro()

                # Atualiza o último estado
                ultimo_estado = estado
                if not self._armazenador.armazena_caso():
                    raise ValueError(f"Erro ao armazenar {self.caso.nome}")
                time.sleep(self.__class__.INTERVALO_POLL)
        except TimeoutError:
            Log.log().error(f"Timeout na execução do caso {self.caso.nome}")
            return False
        except ValueError as e:
            Log.log().error(f"Erro na execução do caso {self.caso.nome}: {e}")
            return False

    @property
    def caso(self) -> Caso:
        return self._caso


class MonitorNEWAVE(MonitorCaso):
    INTERVALO_POLL = 5.0
    MAX_RETRY = 3
    TIMEOUT_COMUNICACAO = 2400.0

    def __init__(self,
                 caso: CasoNEWAVE):
        super().__init__(caso)

    @property
    def caso(self) -> CasoNEWAVE:
        if not isinstance(self._caso, CasoNEWAVE):
            raise ValueError("MonitorNEWAVE tem um caso não de NEWAVE")
        return self._caso

    # Override
    @property
    def caminho_job(self) -> str:
        dir_base = Configuracoes().diretorio_instalacao_newaves
        versao = Configuracoes().versao_newave
        dir_versao = join(dir_base, versao)
        arquivos_versao = listdir(dir_versao)
        arq_job = [a for a in arquivos_versao if ".job" in a]
        return join(dir_versao, arq_job[0])

    # Override
    @property
    def nome_job(self) -> str:
        return f"NW{self.caso.ano}{self.caso.mes}"


class MonitorDECOMP(MonitorCaso):
    INTERVALO_POLL = 5.0
    MAX_RETRY = 3
    TIMEOUT_COMUNICACAO = 300.0

    def __init__(self, caso: CasoDECOMP):
        super().__init__(caso)

    @property
    def caso(self) -> CasoDECOMP:
        if not isinstance(self._caso, CasoDECOMP):
            raise ValueError("MonitorDECOMP tem um caso não de DECOMP")
        return self._caso

    # Override
    @property
    def caminho_job(self) -> str:
        dir_base = Configuracoes().diretorio_instalacao_decomps
        versao = Configuracoes().versao_decomp
        dir_versao = join(dir_base, versao)
        arquivos_versao = listdir(dir_versao)
        arq_job = [a for a in arquivos_versao if ".job" in a]
        return join(dir_versao, arq_job[0])

    # Override
    @property
    def nome_job(self) -> str:
        return f"DC{self.caso.ano}{self.caso.mes}{self.caso.revisao}"
