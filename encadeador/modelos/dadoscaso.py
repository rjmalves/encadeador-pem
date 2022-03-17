from typing import Dict, Any
from os.path import isfile
from os.path import join

NOME_ARQUIVO_ESTADO = "caso_encadeado.json"
INTERVALO_RETRY_ESCRITA = 0.1
MAX_RETRY_ESCRITA = 3


class DadosCaso:
    """
    Dados de execução e gerenciamento de um caso, com o histórico
    de flexibilizações.
    """

    def __init__(
        self,
        _programa: str,
        _caminho: str,
        _nome: str,
        _ano: int,
        _mes: int,
        _revisao: int,
    ) -> None:
        self._programa = _programa
        self._caminho = str(_caminho)
        self._nome = _nome
        self._ano = _ano
        self._mes = _mes
        self._revisao = _revisao

    @staticmethod
    def existem_dados(caminho: str) -> bool:
        arq = join(caminho, NOME_ARQUIVO_ESTADO)
        return isfile(arq)

    @staticmethod
    def from_json(json_dict: Dict[str, Any]) -> "DadosCaso":
        return DadosCaso(**json_dict)

    def to_json(self) -> Dict[str, Any]:
        return self.__dict__

    @property
    def programa(self) -> str:
        return self._programa

    @property
    def caminho(self) -> str:
        return self._caminho

    @caminho.setter
    def caminho(self, c: str):
        self._caminho = c

    @property
    def nome(self) -> str:
        return self._nome

    @property
    def ano(self) -> int:
        return self._ano

    @property
    def mes(self) -> int:
        return self._mes

    @property
    def revisao(self) -> int:
        return self._revisao
