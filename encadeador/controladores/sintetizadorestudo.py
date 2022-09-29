from os.path import join, isdir
from os import makedirs
from typing import List
import pandas as pd  # type: ignore

from encadeador.modelos.caso import Caso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.estudo import Estudo
from encadeador.modelos.programa import Programa
from encadeador.utils.log import Log
from encadeador.adapters.repository.synthesis import (
    factory as synthesis_factory,
)

VARIAVEIS_GERAIS = ["CONVERGENCIA", "TEMPO"]
VARIAVEIS_INVIABILIDADES = [
    "INVIABILIDADES_CODIGO",
    "INVIABILIDADES_PATAMAR",
    "INVIABILIDADES_PATAMAR_LIMITE",
    "INVIABILIDADES_LIMITE",
    "INVIABILIDADES_SBM_PATAMAR",
]
VARIAVEIS_OPERACAO_NEWAVE = ["EARMF_SBM_EST"]
VARIAVEIS_OPERACAO_DECOMP = ["EARMF_SBM_EST"]
QUANTIS = [0.1, 0.9]

COLUNAS_FILTRO = [
    "Submercado",
    "Submercado De",
    "Submercado Para",
    "REE",
    "Usina",
    "Patamar",
    "Estagio",
    "Data Inicio",
    "Data Fim",
]


class SintetizadorEstudo:
    def __init__(self, estudo: Estudo) -> None:
        self._estudo = estudo
        self._diretorio_sintese = join(
            Configuracoes().caminho_base_estudo,
            Configuracoes().diretorio_sintese,
        )
        self._diretorio_newave = join(
            self._diretorio_sintese, Configuracoes().nome_diretorio_newave
        )
        self._diretorio_decomp = join(
            self._diretorio_sintese, Configuracoes().nome_diretorio_decomp
        )
        self.__repositorio_sintese = synthesis_factory(
            Configuracoes().formato_sintese
        )
        if not isdir(self._diretorio_sintese):
            makedirs(self._diretorio_sintese)

    def __casos_nao_sintetizados(
        self, casos_concluidos: List[Caso], arquivo_sintese: str
    ) -> List[Caso]:
        self.__df_sintese = self.__repositorio_sintese.read(arquivo_sintese)
        ids_sintetizados = self.__df_sintese["Caso"].unique().tolist()
        return [c for c in casos_concluidos if c.id not in ids_sintetizados]

    def __sintetiza_adicionando_identificador(
        self, variavel: str, casos: List[Caso], extrai_quantis=False
    ):
        for c in casos:
            arquivo_sintese = join(
                c.caminho, Configuracoes().diretorio_sintese, variavel
            )
            df_caso = self.__repositorio_sintese.read(arquivo_sintese)
            if extrai_quantis:
                df_caso = self.__extrai_quantis_cenarios(df_caso)
            self.__df_sintese = pd.concat(
                [self.__df_sintese, df_caso], ignore_index=True
            )
        self.__repositorio_sintese.write(self.__df_sintese, arquivo_sintese)

    def __extrai_quantis_cenarios(self, df: pd.DataFrame) -> pd.DataFrame:
        cols_cenarios = [
            col for col in df.columns.tolist() if col not in COLUNAS_FILTRO
        ]
        for q in QUANTIS:
            label = f"p{int(100 * q)}"
            df[label] = df[cols_cenarios].quantile(q, axis=1)
        df["median"] = df[cols_cenarios].quantile(0.5, axis=1)
        return df.drop(columns=cols_cenarios)

    def __sintetiza_variavel(
        self,
        diretorio: str,
        variavel: str,
        casos_concluidos: List[Caso],
        extrai_quantis=True,
    ):
        arquivo_v = join(diretorio, variavel)
        nao_sintetizados = self.__casos_nao_sintetizados(
            casos_concluidos, arquivo_v
        )
        self.__sintetiza_adicionando_identificador(
            variavel, nao_sintetizados, extrai_quantis=extrai_quantis
        )
        self.__repositorio_sintese.write(self.__df_sintese, arquivo_v)

    def __sintetiza_newave(self):
        if not isdir(self._diretorio_newave):
            makedirs(self._diretorio_newave)
        newaves_concluidos = [
            c
            for c in self._estudo.casos_concluidos
            if c.programa == Programa.NEWAVE
        ]
        for v in VARIAVEIS_GERAIS:
            self.__sintetiza_variavel(
                self._diretorio_newave,
                v,
                newaves_concluidos,
                extrai_quantis=False,
            )
        for v in VARIAVEIS_OPERACAO_NEWAVE:
            self.__sintetiza_variavel(
                self._diretorio_newave,
                v,
                newaves_concluidos,
                extrai_quantis=True,
            )

    def __sintetiza_decomp(self):
        if not isdir(self._diretorio_decomp):
            makedirs(self._diretorio_decomp)
        decomps_concluidos = [
            c
            for c in self._estudo.casos_concluidos
            if c.programa == Programa.DECOMP
        ]
        for v in VARIAVEIS_GERAIS + VARIAVEIS_INVIABILIDADES:
            self.__sintetiza_variavel(
                self._diretorio_decomp,
                v,
                decomps_concluidos,
                extrai_quantis=False,
            )
        for v in VARIAVEIS_OPERACAO_NEWAVE:
            self.__sintetiza_variavel(
                self._diretorio_decomp,
                v,
                decomps_concluidos,
                extrai_quantis=True,
            )

    def sintetiza_estudo(self) -> bool:
        Log.log().info("Sintetizando resultados do estudo encadeado")
        self.__sintetiza_newave()
        self.__sintetiza_decomp()
        return True
