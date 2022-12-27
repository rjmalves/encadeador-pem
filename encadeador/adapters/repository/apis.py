import aiohttp
from typing import Dict, List, Union
from os.path import join
import json

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
