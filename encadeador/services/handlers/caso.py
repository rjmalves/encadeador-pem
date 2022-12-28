from typing import Optional, Dict, Tuple
import pandas as pd
import pathlib
from os.path import join
from encadeador.controladores.preparadorcaso import PreparadorCaso
from encadeador.adapters.repository.apis import (
    EncadeadorAPIRepository,
    RegrasReservatoriosAPIRepository,
    FlexibilizadorAPIRepository,
)
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.utils.log import Log
from encadeador.internal.httpresponse import HTTPResponse
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.services.unitofwork.caso import AbstractCasoUnitOfWork
from encadeador.services.unitofwork.rodada import AbstractRodadaUnitOfWork
from encadeador.modelos.caso import Caso
from encadeador.modelos.runstatus import RunStatus
from encadeador.modelos.programa import Programa
from encadeador.modelos.reservoirrule import ReservoirRule
import encadeador.domain.commands as commands
from encadeador.domain.programs import ProgramRules
import encadeador.services.handlers.rodada as rodada_handlers

# TODO - no futuro, quando toda a aplicação for
# orientada a eventos, o logging deve ser praticamente
# restrito aos handlers?


def cria(
    command: commands.CriaCaso, uow: AbstractCasoUnitOfWork
) -> Optional[Caso]:
    with uow:
        case_data = ProgramRules.case_from_path(command.caminho)
        if case_data is None:
            return None
        case_name = ProgramRules.case_name_from_data(*case_data)
        if case_name is None:
            return None
        basepath = pathlib.Path(Configuracoes().caminho_base_estudo)
        casepath = pathlib.Path(command.caminho)
        relpath = str(casepath.relative_to(basepath))
        caso = Caso(
            relpath,
            case_name,
            case_data[0],
            case_data[1],
            case_data[2],
            case_data[3],
            EstadoCaso.NAO_INICIADO,
            command.id_estudo,
        )
        if caso is not None:
            uow.casos.create(caso)
            Log.log().info(f"Criando caso {case_name}")
            uow.commit()
        return caso


def inicializa(
    command: commands.InicializaCaso,
    caso_uow: AbstractCasoUnitOfWork,
    rodada_uow: AbstractRodadaUnitOfWork,
) -> Optional[Caso]:
    with rodada_uow:
        # Limpa rodadas do caso que estão ativas
        rodadas_caso = rodada_uow.rodadas.list_by_caso(command.id_caso)
        rodadas_ativas = [r for r in rodadas_caso if r.ativa]
        for r in rodadas_ativas:
            rodada_uow.rodadas.delete(r.id)
        rodada_uow.commit()
    with caso_uow:
        caso = caso_uow.casos.read(command.id_caso)
        if caso is not None:
            caso.estado = EstadoCaso.INICIADO
            caso_uow.commit()
        return caso


async def prepara(
    command: commands.PreparaCaso, uow: AbstractCasoUnitOfWork
) -> Optional[Caso]:
    with uow:
        # Lista os casos anteriores
        caso = uow.casos.read(command.id_caso)
        Log.log().info(f"Caso {caso.nome}: preparando")
        casos_anteriores = [
            c for c in uow.casos.list_by_estudo(caso.id_estudo) if c < caso
        ]
        preparador = PreparadorCaso.factory(caso, casos_anteriores)
        sucesso_prepara = await preparador.prepara()
        sucesso_encadeia = True
        sucesso_regras = True
        # PREMISSA: só encadeia se tiver decomps anteriores.
        if (
            len([c for c in casos_anteriores if c.programa == Programa.DECOMP])
            > 0
        ):
            Log.log().info(f"Caso {caso.nome}: encadeando")
            variaveis = ProgramRules.program_chaining_variables(caso.programa)
            if variaveis is not None:
                for v in variaveis:
                    res = await EncadeadorAPIRepository.encadeia(
                        casos_anteriores, caso, v
                    )
                    if isinstance(res, HTTPResponse):
                        Log.log().warning(
                            "Erro no encadeamento:"
                            + f" [{res.code}] {res.detail}"
                        )
                        sucesso_encadeia = False
                    else:
                        Log.log().info(f"Encadeamento de {v}:")
                        for r in res:
                            Log.log().info(str(r))
            # PREMISSA: só aplica regras de reservatórios
            # se tiver decomps anteriores
            regras_convertidas = [
                ReservoirRule.from_regra(r)
                for r in command.regras_reservatorios
            ]
            if len(regras_convertidas) > 0:
                Log.log().info(
                    f"Caso {caso.nome}: aplicando regras de reservatórios"
                )
                res = await RegrasReservatoriosAPIRepository.aplica_regras(
                    casos_anteriores, caso, regras_convertidas
                )
                if isinstance(res, HTTPResponse):
                    Log.log().warning(
                        "Erro da aplicação de regras de reservatórios:"
                        + f" [{res.code}] {res.detail}"
                    )
                    sucesso_regras = False
                else:
                    Log.log().info("Regras de reservatórios aplicadas:")
                    for r in res:
                        Log.log().info(str(r))

        return all([sucesso_prepara, sucesso_encadeia, sucesso_regras])


async def submete(
    command: commands.SubmeteCaso,
    caso_uow: AbstractCasoUnitOfWork,
    rodada_uow: AbstractRodadaUnitOfWork,
) -> Optional[int]:
    with caso_uow:
        # Extrai o caso
        caso = caso_uow.casos.read(command.id_caso)
        if caso is None:
            Log.log().error(f"Caso {command.id_caso} não encontrado")
            return None
        versao = ProgramRules.program_version(caso.programa)
        processadores = ProgramRules.program_processor_count(caso.programa)
        cmd = commands.CriaRodada(
            caso.programa.value,
            versao,
            join(Configuracoes().caminho_base_estudo, caso.caminho),
            processadores,
            command.id_caso,
        )
        Log.log().info(f"Caso {caso.nome}: submetendo")
        rodada = await rodada_handlers.submete(cmd, rodada_uow)
        if rodada is not None:
            caso.estado = EstadoCaso.EXECUTANDO
            caso_uow.casos.update(caso)
            caso_uow.commit()
            return rodada


async def monitora(
    command: commands.MonitoraCaso,
    caso_uow: AbstractCasoUnitOfWork,
    rodada_uow: AbstractRodadaUnitOfWork,
) -> Optional[TransicaoCaso]:
    cmd = commands.MonitoraRodada(command.id_rodada)
    with caso_uow:
        caso = caso_uow.casos.read(command.id_caso)
        nome = caso.nome
    rodada = await rodada_handlers.monitora(cmd, rodada_uow)
    Log.log().info(f"Monitorando caso {nome}: {rodada.estado.value}")
    if rodada is not None:
        MAPA_ESTADO_TRANSICAO: Dict[RunStatus, TransicaoCaso] = {
            RunStatus.SUCCESS: TransicaoCaso.CONCLUIDO,
            RunStatus.INFEASIBLE: TransicaoCaso.INVIAVEL,
            RunStatus.DATA_ERROR: TransicaoCaso.ERRO_DADOS,
            RunStatus.RUNTIME_ERROR: TransicaoCaso.ERRO_CONVERGENCIA,
            RunStatus.COMMUNICATION_ERROR: TransicaoCaso.ERRO_DADOS,
        }
        return MAPA_ESTADO_TRANSICAO.get(rodada.estado)


def atualiza(
    command: commands.AtualizaCaso, uow: AbstractCasoUnitOfWork
) -> bool:
    with uow:
        caso = uow.casos.read(command.id_caso)
        if caso is not None:
            Log.log().info(f"Caso {caso.nome} -> {command.estado.value}")
            caso.estado = command.estado
            uow.casos.update(caso)
            uow.commit()
        return caso is not None


async def flexibiliza(
    command: commands.FlexibilizaCaso, uow: AbstractCasoUnitOfWork
) -> Optional[TransicaoCaso]:
    with uow:
        caso = uow.casos.read(command.id_caso)
        if caso is not None:
            if caso.numero_flexibilizacoes < command.max_flex:
                Log.log().info(f"Caso {caso.nome}: flexibilizando")
                res = await FlexibilizadorAPIRepository.flexibiliza(caso)
                if isinstance(res, HTTPResponse):
                    Log.log().error(
                        "Erro na flexibilização: "
                        + f"[{res.code}] {res.detail}"
                    )
                    return TransicaoCaso.FLEXIBILIZACAO_ERRO
                else:
                    return TransicaoCaso.FLEXIBILIZACAO_SUCESSO
            else:
                return TransicaoCaso.ERRO_MAX_FLEX


async def sintetiza_casos_rodadas(
    caso_uow: AbstractCasoUnitOfWork, rodada_uow: AbstractRodadaUnitOfWork
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    Log.log().info(f"Sintetizando casos e rodadas")
    with caso_uow:
        casos = caso_uow.casos.list()
        df_casos = pd.DataFrame(
            data={
                "id": [c.id for c in casos],
                "nome": [c.nome for c in casos],
                "caminho": [c.caminho for c in casos],
                "ano": [c.ano for c in casos],
                "mes": [c.mes for c in casos],
                "revisao": [c.revisao for c in casos],
                "programa": [c.programa.value for c in casos],
                "estado": [c.estado.value for c in casos],
                "tempo_execucao": [c.tempo_execucao for c in casos],
                "numero_flexibilizacoes": [
                    c.numero_flexibilizacoes for c in casos
                ],
            }
        )
    df_rodadas = await rodada_handlers.sintetiza_rodadas(rodada_uow)
    return df_casos, df_rodadas


async def corrige_erro_convergencia(
    command: commands.CorrigeErroConvergenciaCaso, uow: AbstractCasoUnitOfWork
) -> bool:
    with uow:
        caso = uow.casos.read(command.id_caso)
        preparador = PreparadorCaso.factory(caso, [])
        return preparador.corrige_erro_convergencia()


async def flexibiliza_criterio_convergencia(
    command: commands.FlexibilizaCriterioConvergenciaCaso,
    uow: AbstractCasoUnitOfWork,
) -> bool:
    with uow:
        caso = uow.casos.read(command.id_caso)
        preparador = PreparadorCaso.factory(caso, [])
        return preparador.flexibiliza_criterio_convergencia()
