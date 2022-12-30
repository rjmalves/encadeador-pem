from os.path import join
from os import makedirs
from typing import List

from encadeador.modelos.caso import Caso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.programa import Programa
from encadeador.utils.log import Log
from encadeador.adapters.repository.synthesis import (
    factory as synthesis_factory,
)
from encadeador.adapters.repository.apis import ResultAPIRepository

VARIAVEIS_GERAIS_NEWAVE = ["CONVERGENCIA", "TEMPO", "CUSTOS"]
VARIAVEIS_GERAIS_DECOMP = ["CONVERGENCIA", "TEMPO", "CUSTOS", "INVIABILIDADES"]

VARIAVEIS_OPERACAO_NEWAVE = ["EARMF_SBM_EST"]
VARIAVEIS_OPERACAO_DECOMP = ["EARMF_SBM_EST"]


class Sintetizador:
    def __init__(self, casos_concluidos: List[Caso]) -> None:
        self.casos_concluidos = casos_concluidos
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
        makedirs(self._diretorio_sintese, exist_ok=True)

    async def sintetiza_newaves(self):
        casos_newave = [
            c for c in self.casos_concluidos if c.programa == Programa.NEWAVE
        ]
        Log.log().info("Realizando síntese dos resultados de NEWAVE")
        makedirs(self._diretorio_newave, exist_ok=True)
        for v in VARIAVEIS_GERAIS_NEWAVE:
            df = await ResultAPIRepository.resultados_1o_estagio_casos(
                casos_newave, v, filtros={}
            )
            if df is None:
                Log.log().info(f"Variável {v} não encontrada")
            else:
                self.__repositorio_sintese.write(
                    df, join(self._diretorio_newave, v)
                )
        for v in VARIAVEIS_OPERACAO_NEWAVE:
            df = await ResultAPIRepository.resultados_1o_estagio_casos(
                casos_newave, v
            )
            if df is None:
                Log.log().info(f"Variável {v} não encontrada")
            else:
                self.__repositorio_sintese.write(
                    df, join(self._diretorio_newave, v)
                )

    async def sintetiza_decomps(self):
        casos_decomp = [
            c for c in self.casos_concluidos if c.programa == Programa.DECOMP
        ]
        Log.log().info("Realizando síntese dos resultados de DECOMP")
        makedirs(self._diretorio_decomp, exist_ok=True)
        for v in VARIAVEIS_GERAIS_DECOMP:
            df = await ResultAPIRepository.resultados_1o_estagio_casos(
                casos_decomp, v, filtros={}
            )
            if df is None:
                Log.log().info(f"Variável {v} não encontrada")
            else:
                self.__repositorio_sintese.write(
                    df, join(self._diretorio_decomp, v)
                )
        for v in VARIAVEIS_OPERACAO_DECOMP:
            df = await ResultAPIRepository.resultados_1o_estagio_casos(
                casos_decomp, v
            )
            if df is None:
                Log.log().info(f"Variável {v} não encontrada")
            else:
                self.__repositorio_sintese.write(
                    df, join(self._diretorio_decomp, v)
                )

    async def sintetiza_resultados(self):
        Log.log().info("Sintetizando resultados do estudo encadeado")
        await self.sintetiza_newaves()
        await self.sintetiza_decomps()
        return True
