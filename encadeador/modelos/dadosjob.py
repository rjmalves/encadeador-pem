import pandas as pd  # type: ignore

class DadosJob:
    """
    Dados de execução e gerenciamento de um job. Em conjunto com a
    classe Job, implementa o padrão Proxy, na forma de proteção
    de acesso (protection proxy).
    """

    def __init__(self,
                 id: str,
                 nome: str,
                 instante_entrada_fila: float,
                 instante_inicio_execucao: float,
                 instante_saida_fila: float,
                 numero_processadores: int) -> None:
        self._id = id
        self._nome = nome
        self._instante_entrada_fila = instante_entrada_fila
        self._instante_inicio_execucao = instante_inicio_execucao
        self._instante_saida_fila = instante_saida_fila
        self._numero_processadores = numero_processadores

    @staticmethod
    def recupera_dados_linha(linha: pd.Series) -> 'DadosJob':

        return DadosJob(linha["ID"],
                        linha["Nome"],
                        linha["Instante Entrada Fila"],
                        linha["Instante Inicio Execucao"],
                        linha["Instante Saida Fila"],
                        linha["Numero Processadores"])

    def resume_dados(self) -> pd.Series:
        return pd.Series([self._id,
                          self._nome,
                          self._instante_entrada_fila,
                          self._instante_inicio_execucao,
                          self._instante_saida_fila,
                          self._numero_processadores],
                          index=["ID",
                                 "Nome",
                                 "Instante Entrada Fila",
                                 "Instante Inicio Execucao",
                                 "Instante Saida Fila",
                                 "Numero Processadores"])

    @property
    def id(self) -> str:
        return self._id

    @property
    def nome(self) -> str:
        return self._nome

    @nome.setter
    def nome(self, n: str):
        self._nome = n

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
    def numero_processadores(self) -> float:
        return self._numero_processadores
