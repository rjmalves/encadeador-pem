from typing import Dict, Any


class DadosJob:
    """
    Dados de execução e gerenciamento de um job. Em conjunto com a
    classe Job, implementa o padrão Proxy, na forma de proteção
    de acesso (protection proxy).
    """

    def __init__(
        self,
        _id: str,
        _nome: str,
        _caminho: str,
        _instante_entrada_fila: float,
        _instante_inicio_execucao: float,
        _instante_saida_fila: float,
        _numero_processadores: int,
    ) -> None:
        self._id = _id
        self._nome = _nome
        self._caminho = str(_caminho)
        self._instante_entrada_fila = _instante_entrada_fila
        self._instante_inicio_execucao = _instante_inicio_execucao
        self._instante_saida_fila = _instante_saida_fila
        self._numero_processadores = _numero_processadores

    @staticmethod
    def from_json(json_dict: Dict[str, Any]) -> "DadosJob":
        return DadosJob(**json_dict)

    def to_json(self) -> Dict[str, Any]:
        return self.__dict__

    @property
    def id(self) -> str:
        return self._id

    @id.setter
    def id(self, i: str):
        self._id = i

    @property
    def nome(self) -> str:
        return self._nome

    @nome.setter
    def nome(self, n: str):
        self._nome = n

    @property
    def caminho(self):
        return self._caminho

    @property
    def instante_entrada_fila(self) -> float:
        return self._instante_entrada_fila

    @instante_entrada_fila.setter
    def instante_entrada_fila(self, i: float):
        self._instante_entrada_fila = i

    @property
    def instante_inicio_execucao(self) -> float:
        return self._instante_inicio_execucao

    @instante_inicio_execucao.setter
    def instante_inicio_execucao(self, i: float):
        self._instante_inicio_execucao = i

    @property
    def instante_saida_fila(self) -> float:
        return self._instante_saida_fila

    @instante_saida_fila.setter
    def instante_saida_fila(self, i: float):
        self._instante_saida_fila = i

    @property
    def numero_processadores(self) -> int:
        return self._numero_processadores

    @numero_processadores.setter
    def numero_processadores(self, n: int):
        self._numero_processadores = n
