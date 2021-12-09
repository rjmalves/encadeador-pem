import pandas as pd  # type: ignore
import time
from typing import List
from os.path import isfile
from os.path import join

from encadeador.modelos.estadojob import EstadoJob

NOME_ARQUIVO_ESTADO = "caso_encadeado.csv"
INTERVALO_RETRY_ESCRITA = 0.1
MAX_RETRY_ESCRITA = 3


class DadosCaso:
    """
    Dados de execução e gerenciamento de um caso, com o histórico
    de flexibilizações.
    """

    def __init__(self,
                 programa: str,
                 caminho: str,
                 nome: str,
                 ano: int,
                 mes: int,
                 revisao: int,
                 estados: List[EstadoJob],
                 instantes_entrada_fila: List[float],
                 instantes_inicio_execucao: List[float],
                 instantes_fim_execucao: List[float],
                 numeros_tentativas: List[int],
                 numeros_processadores: List[int],
                 sucessos: List[bool],
                 numero_flexibilizacoes: int) -> None:
        self._programa = programa
        self._caminho = caminho
        self._nome = nome
        self._ano = ano
        self._mes = mes
        self._revisao = revisao
        self._estados = estados
        self._instantes_entrada_fila = instantes_entrada_fila
        self._instantes_inicio_execucao = instantes_inicio_execucao
        self._instantes_fim_execucao = instantes_fim_execucao
        self._numeros_tentativas = numeros_tentativas
        self._numeros_processadores = numeros_processadores
        self._sucessos = sucessos
        self._numero_flexibilizacoes = numero_flexibilizacoes

    @staticmethod
    def existem_dados(caminho: str) -> bool:
        arq = join(caminho, NOME_ARQUIVO_ESTADO)
        return isfile(arq)

    @staticmethod
    def le_arquivo(caminho: str) -> 'DadosCaso':

        def __extrai_coluna_em_lista(df: pd.DataFrame, coluna: str):
            return df.loc[:, coluna].tolist()

        # Se não tem arquivo de resumo, o caso não começou a ser rodado
        if not DadosCaso.existem_dados(caminho):
            raise FileNotFoundError("Não encontrado arquivo de resumo" +
                                    f" de caso no diretório {caminho}.")

        arq = join(caminho, NOME_ARQUIVO_ESTADO)
        df = pd.read_csv(arq, index_col=0)
        n_flexibilizacoes, _ = df.shape
        estados = [EstadoJob.factory(e) for e in
                   __extrai_coluna_em_lista(df, "Estado")]
        sucessos = [bool(int(s)) for s in
                    __extrai_coluna_em_lista(df, "Sucesso")]
        return DadosCaso(__extrai_coluna_em_lista(df, "Programa")[0],
                         __extrai_coluna_em_lista(df, "Caminho")[0],
                         __extrai_coluna_em_lista(df, "Nome")[0],
                         __extrai_coluna_em_lista(df, "Ano")[0],
                         __extrai_coluna_em_lista(df, "Mes")[0],
                         __extrai_coluna_em_lista(df, "Revisao")[0],
                         estados,
                         __extrai_coluna_em_lista(df, "Entrada Fila"),
                         __extrai_coluna_em_lista(df, "Inicio Execucao"),
                         __extrai_coluna_em_lista(df, "Fim Execucao"),
                         __extrai_coluna_em_lista(df, "Tentativas"),
                         __extrai_coluna_em_lista(df, "Processadores"),
                         sucessos,
                         n_flexibilizacoes - 1)

    @staticmethod
    def obtem_dados_do_caso(prog: str,
                            caminho: str,
                            nome: str,
                            ano: int,
                            mes: int,
                            revisao: int,
                            numero_processadores: int) -> 'DadosCaso':
        if prog not in ["NEWAVE", "DECOMP"]:
            raise ValueError(f"Caso do tipo {prog} não suportado")
        return DadosCaso(prog,
                         caminho,
                         nome,
                         ano,
                         mes,
                         revisao,
                         [EstadoJob.NAO_INICIADO],
                         [0],
                         [0],
                         [0],
                         [0],
                         [numero_processadores],
                         [False],
                         0)

    def escreve_arquivo(self):
        num_retry = 0
        while num_retry < MAX_RETRY_ESCRITA:
            try:
                self.df_dados.to_csv(join(self.caminho, NOME_ARQUIVO_ESTADO),
                                     header=True,
                                     encoding="utf-8",
                                     )
                return
            except OSError:
                num_retry += 1
                time.sleep(INTERVALO_RETRY_ESCRITA)
                continue
            except BlockingIOError:
                num_retry += 1
                time.sleep(INTERVALO_RETRY_ESCRITA)
                continue

    def adiciona_flexibilizacao(self):
        if self.df_dados.shape[0] == 0:
            raise ValueError("Caso não inicializado. " +
                             "Não é possível flexibilizar")
        # Incrementa o número de flex e inicia com valores default
        # as listas
        self._numero_flexibilizacoes += 1
        self._estados.append(EstadoJob.NAO_INICIADO)
        self._numeros_tentativas.append(0)
        self._numeros_processadores.append(self._numeros_processadores[-1])
        self._sucessos.append(False)
        self._instantes_entrada_fila.append(0)
        self._instantes_inicio_execucao.append(0)
        self._instantes_fim_execucao.append(0)

    @property
    def df_dados(self) -> pd.DataFrame:
        n = self._numero_flexibilizacoes + 1
        dados = {
                 "Programa": [self._programa] * n,
                 "Caminho": [self._caminho] * n,
                 "Nome": [self._nome] * n,
                 "Ano": [self._ano] * n,
                 "Mes": [self._mes] * n,
                 "Revisao": [self._revisao] * n,
                 "Estado": [str(e.value) for e in self._estados],
                 "Tentativas": self._numeros_tentativas,
                 "Processadores": self._numeros_processadores,
                 "Sucesso": [int(s) for s in self._sucessos],
                 "Entrada Fila": self._instantes_entrada_fila,
                 "Inicio Execucao": self._instantes_inicio_execucao,
                 "Fim Execucao": self._instantes_fim_execucao
                }
        df = pd.DataFrame(data=dados)
        return df

    @property
    def programa(self) -> str:
        return self._programa

    @property
    def caminho(self) -> str:
        return self._caminho

    @caminho.setter
    def caminho(self, c: str) -> str:
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

    @property
    def estado(self) -> EstadoJob:
        return self._estados[-1]

    @estado.setter
    def estado(self, e: EstadoJob):
        self._estados[-1] = e

    @property
    def numero_tentativas(self) -> int:
        return self._numeros_tentativas[-1]

    @numero_tentativas.setter
    def numero_tentativas(self, n: int):
        self._numeros_tentativas[-1] = n

    @property
    def numero_processadores(self) -> int:
        return self._numeros_processadores[-1]

    @numero_processadores.setter
    def numero_processadores(self, n: int):
        self._numeros_processadores[-1] = n

    @property
    def sucesso(self) -> bool:
        return self._sucessos[-1]

    @sucesso.setter
    def sucesso(self, s: bool):
        self._sucessos[-1] = s

    @property
    def instante_entrada_fila(self) -> float:
        return self._instantes_entrada_fila[-1]

    @instante_entrada_fila.setter
    def instante_entrada_fila(self, t: float):
        self._instantes_entrada_fila[-1] = t

    @property
    def instante_inicio_execucao(self) -> float:
        return self._instantes_inicio_execucao[-1]

    @instante_inicio_execucao.setter
    def instante_inicio_execucao(self, t: float):
        self._instantes_inicio_execucao[-1] = t

    @property
    def instante_fim_execucao(self) -> float:
        return self._instantes_fim_execucao[-1]

    @instante_fim_execucao.setter
    def instante_fim_execucao(self, t: float):
        self._instantes_fim_execucao[-1] = t

    @property
    def numero_flexibilizacoes(self) -> int:
        return self._numero_flexibilizacoes
