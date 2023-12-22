from typing import Optional
import pandas as pd  # type: ignore
from encadeador.adapters.repository.apis import ModelAPIRepository
from encadeador.services.unitofwork.rodada import AbstractRodadaUnitOfWork
from encadeador.modelos.run import Run
from encadeador.modelos.rodada import Rodada
from encadeador.internal.httpresponse import HTTPResponse
from encadeador.utils.log import Log
import encadeador.domain.commands as commands


async def submete(
    command: commands.CriaRodada, uow: AbstractRodadaUnitOfWork
) -> Optional[int]:
    with uow:
        run = Run(
            runId=None,
            status=None,
            name=None,
            jobId=None,
            jobWorkingDirectory=command.caminho,
            jobStartTime=None,
            jobEndTime=None,
            jobReservedSlots=command.numero_processadores,
            jobArgs=[str(command.numero_processadores)],
            programName=command.programa,
            programVersion=command.versao,
        )
        res = await ModelAPIRepository.create_run(run)
        Log.log().info(f"ID da rodada: {res}")
        if isinstance(res, HTTPResponse):
            Log.log().warning(
                f"Erro na submissão {str(run)}: [{res.code}] {res.detail}"
            )
            return None
        createdRun = await ModelAPIRepository.read_run(res)
        if isinstance(createdRun, HTTPResponse):
            Log.log().warning(
                "Erro na leitura da leitura criada:"
                + f" [{createdRun.code}] {createdRun.detail}"
            )
            return None
        Log.log().info(f"Criada rodada {str(createdRun)}")
        rodada = Rodada.from_run(createdRun, command.id_caso)
        uow.rodadas.create(rodada)
        uow.commit()
        return createdRun.runId


async def monitora(
    command: commands.MonitoraRodada,
    uow: AbstractRodadaUnitOfWork,
) -> Optional[Rodada]:
    with uow:
        res = await ModelAPIRepository.read_run(command.id)
        if isinstance(res, HTTPResponse):
            Log.log().warning(
                f"Erro no monitoramento [rodada {command.id}]:"
                + f" [{res.code}] {res.detail}"
            )
            return None
        rodada = uow.rodadas.read(command.id)
        if rodada is not None:
            rodada_from_api = Rodada.from_run(res, rodada.id_caso)
            uow.rodadas.update(rodada_from_api)
            uow.commit()
            return rodada_from_api
        else:
            Log.log().warning(
                f"Erro na atualização: rodada {command.id} não encontrada"
            )
            return None


async def deleta(
    command: commands.DeletaRodada, uow: AbstractRodadaUnitOfWork
) -> bool:
    with uow:
        rodada = uow.rodadas.read(command.id)
        if rodada is not None:
            res = await ModelAPIRepository.delete_run(command.id)
            if res.code != 202:
                Log.log().warning(
                    f"Erro na deleção [rodada {command.id}]:"
                    + f" [{res.code}] {res.detail}"
                )
                return False
            uow.rodadas.delete(command.id)
            uow.commit()
        return True


async def sintetiza_rodadas(uow: AbstractRodadaUnitOfWork) -> pd.DataFrame:
    with uow:
        rodadas = uow.rodadas.list()
        return pd.DataFrame(
            data={
                "id": [c.id for c in rodadas],
                "nome": [c.nome for c in rodadas],
                "caminho": [c.caminho for c in rodadas],
                "id_job": [c.id_job for c in rodadas],
                "instante_inicio_execucao": [
                    c.instante_inicio_execucao for c in rodadas
                ],
                "instante_fim_execucao": [
                    c.instante_fim_execucao for c in rodadas
                ],
                "numero_processadores": [
                    c.numero_processadores for c in rodadas
                ],
                "estado": [c.estado.value for c in rodadas],
                "nome_programa": [c.nome_programa for c in rodadas],
                "versao_programa": [c.versao_programa for c in rodadas],
                "id_caso": [c.id_caso for c in rodadas],
            }
        )
