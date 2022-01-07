from os.path import isfile
from os.path import join

from encadeador.modelos.dadoscaso import DadosCaso
from encadeador.modelos.caso import Caso
from encadeador.utils.log import Log

NOME_ARQUIVO_ESTADO = "caso_encadeado.csv"


class ArmazenadorCaso:

    def __init__(self,
                 caso: Caso) -> None:
        self._caso = caso

    def armazena_caso(self) -> bool:
        try:
            self._caso._dados.escreve_arquivo()
            return True
        except Exception as e:
            Log.log().error("Erro no armazenamento do caso" +
                            f" {self._caso.nome}: {e}")
            return False

    @staticmethod
    def recupera_caso(caminho: str) -> Caso:

        # Se não tem arquivo de resumo, o caso não começou a ser rodado
        arq = join(caminho, NOME_ARQUIVO_ESTADO)
        if not isfile(arq):
            raise FileNotFoundError("Não encontrado arquivo de resumo" +
                                    f" de caso no diretório {caminho}.")

        # Se tem, então o caso pelo menos começou
        dados = DadosCaso.le_arquivo(caminho)
        c = Caso.factory(dados.programa)
        c.recupera_caso_dos_dados(dados)
        return c

    @property
    def caso(self) -> Caso:
        return self._caso
