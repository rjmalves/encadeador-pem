from os.path import join
from os import makedirs
from typing import List
import pandas as pd  # type: ignore

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

VARIAVEIS_OPERACAO_NEWAVE = [
    "CMO_SBM_EST",
    "CMO_SBM_PAT",
    "VAGUA_REE_EST",
    "VAGUA_UHE_EST",
    "VAGUAI_UHE_EST",
    "CTER_SBM_EST",
    "CTER_SIN_EST",
    "COP_SIN_EST",
    "ENAA_REE_EST",
    "ENAA_SBM_EST",
    "ENAA_SIN_EST",
    "EARPF_REE_EST",
    "EARPF_SBM_EST",
    "EARPF_SIN_EST",
    "EARMF_REE_EST",
    "EARMF_SBM_EST",
    "EARMF_SIN_EST",
    "GHID_REE_EST",
    "GHID_SBM_EST",
    "GHID_SIN_EST",
    "GTER_UTE_EST",
    "GTER_SBM_EST",
    "GTER_SIN_EST",
    "GHID_REE_PAT",
    "GHID_SBM_PAT",
    "GHID_SIN_PAT",
    "GTER_SBM_PAT",
    "GTER_SIN_PAT",
    "EVER_REE_EST",
    "EVER_SBM_EST",
    "EVER_SIN_EST",
    "EVERR_REE_EST",
    "EVERR_SBM_EST",
    "EVERR_SIN_EST",
    "EVERF_REE_EST",
    "EVERF_SBM_EST",
    "EVERF_SIN_EST",
    "EVERFT_REE_EST",
    "EVERFT_SBM_EST",
    "EVERFT_SIN_EST",
    "QAFL_UHE_EST",
    "QINC_UHE_EST",
    "QDEF_UHE_EST",
    "QDEF_UHE_PAT",
    "QTUR_UHE_EST",
    "QTUR_UHE_PAT",
    "QVER_UHE_EST",
    "QVER_UHE_PAT",
    "VARMF_UHE_EST",
    "VARMF_REE_EST",
    "VARMF_SBM_EST",
    "VARMF_SIN_EST",
    "VARPF_UHE_EST",
    "GHID_UHE_PAT",
    "GHID_UHE_EST",
    "INT_SBP_EST",
    "INT_SBP_PAT",
    "DEF_SBM_EST",
    "DEF_SBM_PAT",
    "DEF_SIN_EST",
    "DEF_SIN_PAT",
    "CDEF_SBM_EST",
    "CDEF_SIN_EST",
    "VDEFMIN_UHE_EST",
    "VDEFMIN_REE_EST",
    "VDEFMIN_SBM_EST",
    "VDEFMIN_SIN_EST",
    "VVMINOP_REE_EST",
    "VVMINOP_SBM_EST",
    "VVMINOP_SIN_EST",
]
VARIAVEIS_OPERACAO_DECOMP = [
    "CMO_SBM_EST",
    "CTER_SIN_EST",
    "COP_SIN_EST",
    "CFU_SIN_EST",
    "EARMI_SBM_EST",
    "EARMI_SIN_EST",
    "EARPI_SBM_EST",
    "EARPI_SIN_EST",
    "EARMF_SBM_EST",
    "EARMF_SIN_EST",
    "EARPF_SBM_EST",
    "EARPF_SIN_EST",
    "GTER_SBM_EST",
    "GTER_SBM_PAT",
    "GTER_SIN_EST",
    "GTER_SIN_PAT",
    "GHID_UHE_EST",
    "GHID_UHE_PAT",
    "GHID_SBM_EST",
    "GHID_SBM_PAT",
    "GHID_SIN_EST",
    "GHID_SIN_PAT",
    "ENAA_SBM_EST",
    "ENAA_SIN_EST",
    "ENAM_SBM_EST",
    "MER_SBM_EST",
    "MER_SBM_PAT",
    "MER_SIN_EST",
    "MER_SIN_PAT",
    "DEF_SBM_EST",
    "DEF_SBM_PAT",
    "DEF_SIN_EST",
    "DEF_SIN_PAT",
    "VARPI_UHE_EST",
    "VARPF_UHE_EST",
    "VARMI_UHE_EST",
    "VARMF_UHE_EST",
    "VARMI_REE_EST",
    "VARMF_REE_EST",
    "VARMI_SBM_EST",
    "VARMF_SBM_EST",
    "VARMI_SIN_EST",
    "VARMF_SIN_EST",
    "QINC_UHE_EST",
    "QAFL_UHE_EST",
    "QDEF_UHE_EST",
    "QTUR_UHE_EST",
    "QVER_UHE_EST",
    "GHID_UHE_EST",
    "GHID_UHE_PAT",
    "EVERT_UHE_EST",
    "EVERNT_UHE_EST",
    "EVERT_REE_EST",
    "EVERNT_REE_EST",
    "EVERT_SBM_EST",
    "EVERNT_SBM_EST",
    "EVERT_SIN_EST",
    "EVERNT_SIN_EST",
    "GTER_UTE_EST",
    "GTER_UTE_PAT",
    "CTER_UTE_EST",
    "INT_SBP_EST",
    "INT_SBP_PAT",
]


class Sintetizador:
    def __init__(self, casos_concluidos: List[Caso]) -> None:
        self.casos_concluidos = casos_concluidos
        self._diretorio_sintese = join(
            Configuracoes().caminho_base_estudo,
            Configuracoes().diretorio_sintese,
        )
        self._diretorio_newave = join(
            self._diretorio_sintese,
            Configuracoes().nome_diretorio_newave,
            Configuracoes().diretorio_sintese,
        )
        self._diretorio_decomp = join(
            self._diretorio_sintese,
            Configuracoes().nome_diretorio_decomp,
            Configuracoes().diretorio_sintese,
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
            Log.log().info(f"Sintetizando {v}")
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
            Log.log().info(f"Sintetizando {v}")
            # Filtra quais casos ainda não foram sintetizados
            caminho_sintese = join(self._diretorio_newave, v)
            try:
                sintese_atual = self.__repositorio_sintese.read(
                    caminho_sintese
                )
            except FileNotFoundError:
                sintese_atual = None
            if sintese_atual is not None:
                ano_mes_rv_sintetizados = sintese_atual["caso"].unique()
                ano_mes_rv_casos = [
                    f"{c.ano}_{str(c.mes).zfill(2)}_rv{c.revisao}"
                    for c in casos_newave
                ]
                casos_faltantes = [
                    c
                    for a, c in zip(ano_mes_rv_casos, casos_newave)
                    if a not in ano_mes_rv_sintetizados
                ]
                casos_faltantes_log = [
                    a
                    for a in ano_mes_rv_casos
                    if a not in ano_mes_rv_sintetizados
                ]
                Log.log().info(f"Casos faltantes: {casos_faltantes_log}")
                df = await ResultAPIRepository.resultados_1o_estagio_casos(
                    casos_faltantes, v
                )
                df = pd.concat([sintese_atual, df], ignore_index=True)
            else:
                Log.log().info("Casos faltantes: todos.")
                df = await ResultAPIRepository.resultados_1o_estagio_casos(
                    casos_newave, v
                )
            if df is None:
                Log.log().info(f"Variável {v} não encontrada")
            else:
                self.__repositorio_sintese.write(df, caminho_sintese)

    async def sintetiza_decomps(self):
        casos_decomp = [
            c for c in self.casos_concluidos if c.programa == Programa.DECOMP
        ]
        Log.log().info("Realizando síntese dos resultados de DECOMP")
        makedirs(self._diretorio_decomp, exist_ok=True)
        for v in VARIAVEIS_GERAIS_DECOMP:
            Log.log().info(f"Sintetizando {v}")
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
            Log.log().info(f"Sintetizando {v}")
            # Filtra quais casos ainda não foram sintetizados
            caminho_sintese = join(self._diretorio_decomp, v)
            try:
                sintese_atual = self.__repositorio_sintese.read(
                    caminho_sintese
                )
            except FileNotFoundError:
                sintese_atual = None
            if sintese_atual is not None:
                ano_mes_rv_sintetizados = sintese_atual["caso"].unique()
                ano_mes_rv_casos = [
                    f"{c.ano}_{str(c.mes).zfill(2)}_rv{c.revisao}"
                    for c in casos_decomp
                ]
                casos_faltantes = [
                    c
                    for a, c in zip(ano_mes_rv_casos, casos_decomp)
                    if a not in ano_mes_rv_sintetizados
                ]
                casos_faltantes_log = [
                    a
                    for a in ano_mes_rv_casos
                    if a not in ano_mes_rv_sintetizados
                ]
                Log.log().info(f"Casos faltantes: {casos_faltantes_log}")
                df = await ResultAPIRepository.resultados_1o_estagio_casos(
                    casos_faltantes, v
                )
                df = pd.concat([sintese_atual, df], ignore_index=True)
            else:
                Log.log().info("Casos faltantes: todos")
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
