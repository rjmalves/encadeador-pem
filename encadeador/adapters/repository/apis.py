import aiohttp
from typing import Dict, List, Union, Optional
from os.path import join
import json
import asyncio
import io
import pathlib
import pandas as pd

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.internal.httpresponse import HTTPResponse
from encadeador.modelos.run import Run
from encadeador.modelos.chainingresult import ChainingResult
from encadeador.modelos.flexibilizationresult import FlexibilizationResult
from encadeador.modelos.reservoirrule import ReservoirRule
from encadeador.modelos.reservoirgrouprule import ReservoirGroupRule
from encadeador.modelos.caso import Caso
from encadeador.utils.log import Log
from encadeador.utils.url import base62_encode


class ModelAPIRepository:
    @staticmethod
    async def list_runs() -> Union[List[Run], HTTPResponse]:
        async with aiohttp.ClientSession() as session:
            url = Configuracoes().model_api + "runs/"
            async with session.get(url) as r:
                if r.status != 200:
                    return HTTPResponse(code=r.status, detail=await r.text())
                else:
                    jobData = await r.json()
                    return [Run.parse_raw(j) for j in jobData]

    @staticmethod
    async def read_run(runId: int) -> Union[Run, HTTPResponse]:
        async with aiohttp.ClientSession() as session:
            url = Configuracoes().model_api + "runs/" + str(runId)
            async with session.get(url) as r:
                if r.status != 200:
                    return HTTPResponse(code=r.status, detail=await r.text())
                else:
                    jobData = await r.json()
                    return Run.parse_raw(jobData)

    @staticmethod
    async def create_run(run: Run) -> Union[int, HTTPResponse]:
        async with aiohttp.ClientSession() as session:
            url = Configuracoes().model_api + "runs/"
            async with session.post(url, json=json.loads(run.json())) as r:
                if r.status != 201:
                    return HTTPResponse(code=r.status, detail=await r.text())
                else:
                    rundata = await r.json()
                    return rundata["runId"]

    @staticmethod
    async def delete_run(runId: int) -> HTTPResponse:
        async with aiohttp.ClientSession() as session:
            url = Configuracoes().model_api + "runs/" + str(runId)
            async with session.delete(url) as r:
                return HTTPResponse(code=r.status, detail=await r.text())


class EncadeadorAPIRepository:
    @staticmethod
    async def encadeia(
        casos_anteriores: List[Caso], caso_destino: Caso, variavel: str
    ) -> Union[List[ChainingResult], HTTPResponse]:
        req = {
            "sources": [
                {
                    "id": base62_encode(
                        join(
                            Configuracoes().caminho_base_estudo,
                            c.caminho,
                        )
                    ),
                    "program": c.programa.value,
                }
                for c in casos_anteriores
            ],
            "destination": {
                "id": base62_encode(
                    join(
                        Configuracoes().caminho_base_estudo,
                        caso_destino.caminho,
                    )
                ),
                "program": caso_destino.programa.value,
            },
            "variable": variavel,
        }
        async with aiohttp.ClientSession() as session:
            url = Configuracoes().encadeador_service
            async with session.post(url, json=req) as r:
                if r.status != 200:
                    return HTTPResponse(code=r.status, detail=await r.text())
                else:
                    chainData = await r.json()
                    return [ChainingResult.parse_raw(j) for j in chainData]


class FlexibilizadorAPIRepository:
    @staticmethod
    async def flexibiliza(
        caso: Caso,
    ) -> Union[List[FlexibilizationResult], HTTPResponse]:
        req = {
            "id": base62_encode(
                join(
                    Configuracoes().caminho_base_estudo,
                    caso.caminho,
                )
            ),
            "program": caso.programa.value,
        }
        async with aiohttp.ClientSession() as session:
            url = Configuracoes().flexibilizador_service
            async with session.post(url, json=req) as r:
                if r.status != 200:
                    return HTTPResponse(code=r.status, detail=await r.text())
                else:
                    flexData = await r.json()
                    return [
                        FlexibilizationResult.parse_raw(j) for j in flexData
                    ]


class RegrasReservatoriosAPIRepository:
    @staticmethod
    async def aplica_regras(
        casos_anteriores: List[Caso],
        caso_destino: Caso,
        regras: List[ReservoirRule],
    ) -> Union[List[ReservoirGroupRule], HTTPResponse]:
        req = {
            "sources": [
                {
                    "id": base62_encode(
                        join(
                            Configuracoes().caminho_base_estudo,
                            c.caminho,
                        )
                    ),
                    "program": c.programa.value,
                }
                for c in casos_anteriores
            ],
            "destination": {
                "id": base62_encode(
                    join(
                        Configuracoes().caminho_base_estudo,
                        caso_destino.caminho,
                    )
                ),
                "program": caso_destino.programa.value,
            },
            "rules": [json.loads(r.json()) for r in regras],
        }
        async with aiohttp.ClientSession() as session:
            url = Configuracoes().regras_reservatorios_service
            async with session.post(url, json=req) as r:
                if r.status != 200:
                    return HTTPResponse(code=r.status, detail=await r.text())
                else:
                    flexData = await r.json()
                    return [
                        FlexibilizationResult.parse_raw(j) for j in flexData
                    ]


class ResultAPIRepository:
    @staticmethod
    async def resultados_1o_estagio_casos(
        casos: List[Caso],
        variavel: str,
        filtros: dict = {"estagio": 1, "cenario": "mean"},
    ) -> Optional[pd.DataFrame]:
        valid_dfs: List[pd.DataFrame] = []
        async with aiohttp.ClientSession() as session:
            ret: List[Optional[pd.DataFrame]] = await asyncio.gather(
                *[
                    ResultAPIRepository.resultados_caso(
                        session,
                        join(Configuracoes().caminho_base_estudo, c.caminho),
                        variavel,
                        filtros,
                    )
                    for c in casos
                ]
            )
            for c, df in zip(casos, ret):
                ano_mes_rv = pathlib.Path(c).parts[-2]
                if df is not None:
                    df_cols = df.columns.to_list()
                    df["caso"] = ano_mes_rv
                    df = df[["caso"] + df_cols]
                    valid_dfs.append(df)
        if len(valid_dfs) > 0:
            complete_df = pd.concat(valid_dfs, ignore_index=True)
            return complete_df
        else:
            return None

    @classmethod
    async def resultados_caso(
        cls,
        session: aiohttp.ClientSession,
        case_path: str,
        desired_data: str,
        filters: dict,
    ) -> Optional[pd.DataFrame]:
        identifier = base62_encode(case_path)
        url = f"{Configuracoes().result_api}/{identifier}/{desired_data}"
        async with session.get(url, params=filters) as r:
            if r.status != 200:
                return None
            else:
                return pd.read_parquet(io.BytesIO(await r.content.read()))
