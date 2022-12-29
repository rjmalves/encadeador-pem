from typing import Optional
import pandas as pd  # type: ignore
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estudo import Estudo
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.controladores.sintetizador import Sintetizador
from encadeador.services.unitofwork.caso import AbstractCasoUnitOfWork
from encadeador.services.unitofwork.estudo import AbstractEstudoUnitOfWork
import encadeador.services.handlers.caso as handlers_caso
import encadeador.domain.commands as commands
from encadeador.utils.log import Log

# TODO - no futuro, quando toda a aplicação for
# orientada a eventos, o logging deve ser praticamente
# restrito aos handlers?


def cria(
    command: commands.CriaEstudo, uow: AbstractEstudoUnitOfWork
) -> Optional[Estudo]:
    with uow:
        estudo = Estudo(
            str(command.caminho),
            command.nome,
            EstadoEstudo.NAO_INICIADO,
        )
        if estudo is not None:
            uow.estudos.create(estudo)
            Log.log().info(f"Criando estudo {command.nome}")
            uow.commit()
        return estudo


def inicializa(
    command: commands.InicializaEstudo,
    estudo_uow: AbstractEstudoUnitOfWork,
    caso_uow: AbstractCasoUnitOfWork,
) -> Optional[Estudo]:
    with caso_uow:
        casos_existentes = caso_uow.casos.list_by_estudo(command.id_estudo)
        for d in command.diretorios_casos:
            if len([c for c in casos_existentes if c.caminho == d]) == 0:
                comando_cria_caso = commands.CriaCaso(d, command.id_estudo)
                handlers_caso.cria(comando_cria_caso, caso_uow)
    with estudo_uow:
        estudo = estudo_uow.estudos.read(command.id_estudo)
        if estudo is not None:
            estudo.estado = EstadoEstudo.INICIADO
            estudo_uow.commit()
        return estudo


async def monitora(
    command: commands.MonitoraEstudo,
    monitor: MonitorCaso,
):
    await monitor.monitora()


def atualiza(
    command: commands.AtualizaEstudo, uow: AbstractEstudoUnitOfWork
) -> bool:
    with uow:
        estudo = uow.estudos.read(command.id_estudo)
        if estudo is not None:
            estudo.estado = command.estado
            uow.estudos.update(estudo)
        return estudo is not None


async def sintetiza_estudo(uow: AbstractEstudoUnitOfWork) -> pd.DataFrame:
    with uow:
        estudos = uow.estudos.list()
        return pd.DataFrame(
            data={
                "id": [c.id for c in estudos],
                "nome": [c.nome for c in estudos],
                "caminho": [c.caminho for c in estudos],
                "estado": [c.estado.value for c in estudos],
            }
        )


async def sintetiza_resultados(
    command: commands.SintetizaEstudo, uow: AbstractEstudoUnitOfWork
):
    with uow:
        estudo = uow.estudos.read(command.id_estudo)
        if estudo is None:
            Log.log().error("Erro ao acessar estudo para síntese")
        else:
            sintetizador = Sintetizador(estudo.casos_concluidos)
            await sintetizador.sintetiza_resultados()
