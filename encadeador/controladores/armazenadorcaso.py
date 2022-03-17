from os.path import isfile
from os.path import join

from encadeador.modelos.caso import Caso
from encadeador.utils.log import Log
from encadeador.utils.io import le_arquivo_json, escreve_arquivo_json

NOME_ARQUIVO_ESTADO = "caso_encadeado.json"


class ArmazenadorCaso:
    def __init__(self, caso: Caso) -> None:
        self._caso = caso

    def armazena_caso(self) -> bool:
        try:
            caminho = join(self._caso.caminho, NOME_ARQUIVO_ESTADO)
            dados = self._caso.to_json()
            escreve_arquivo_json(caminho, dados)
            return True
        except Exception as e:
            Log.log().error(
                "Erro no armazenamento do caso" + f" {self._caso.nome}: {e}"
            )
            return False

    @staticmethod
    def recupera_caso(caminho: str) -> Caso:

        # Se não tem arquivo de resumo, o caso não começou a ser rodado
        arq = join(caminho, NOME_ARQUIVO_ESTADO)
        if not isfile(arq):
            raise FileNotFoundError(
                "Não encontrado arquivo de resumo"
                + f" de caso no diretório {caminho}."
            )

        # Se tem, então o caso pelo menos começou
        return Caso.from_json(le_arquivo_json(arq))

    @property
    def caso(self) -> Caso:
        return self._caso
