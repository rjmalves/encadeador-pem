import pandas as pd  # type: ignore
from logging import Logger
from os.path import isfile
from os.path import join

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.dadoscaso import DadosCaso
from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE

NOME_ARQUIVO_ESTADO = "caso_encadeado.csv"


class ArmazenadorCaso:

    def __init__(self,
                 caso: Caso,
                 log: Logger) -> None:
        self._caso = caso
        self._log = log

    def armazena_caso(self, estado: EstadoJob) -> bool:
        try:
            self._caso._dados.escreve_arquivo()
            return True
        except Exception as e:
            self._log.error("Erro no armazenamento do caso" +
                            f" {self._caso.nome}: {e}")
            return False

    @staticmethod
    def recupera_caso(cfg: Configuracoes,
                      caminho: str) -> Caso:

        # Se não tem arquivo de resumo, o caso não começou a ser rodado
        arq = join(caminho, NOME_ARQUIVO_ESTADO)
        if not isfile(arq):
            raise FileNotFoundError("Não encontrado arquivo de resumo" +
                                    f" de caso no diretório {caminho}.")

        # Se tem, então o caso pelo menos começou
        dados = DadosCaso.le_arquivo(caminho)
        c = Caso.factory(dados.programa)
        c.recupera_caso_dos_dados(dados, cfg)
        return c

    @property
    def caso(self) -> Caso:
        return self._caso
