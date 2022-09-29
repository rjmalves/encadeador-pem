from typing import Optional

from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estudo import Estudo
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.controladores.sintetizadorestudo import SintetizadorEstudo
from encadeador.services.unitofwork.caso import AbstractCasoUnitOfWork
from encadeador.services.unitofwork.estudo import AbstractEstudoUnitOfWork
import encadeador.services.handlers.caso as handlers_caso
import encadeador.domain.commands as commands
from encadeador.domain.programs import ProgramRules

# TODO - no futuro, quando toda a aplicação for
# orientada a eventos, o logging deve ser praticamente
# restrito aos handlers?


def cria(
    command: commands.CriaEstudo, uow: AbstractEstudoUnitOfWork
) -> Optional[Estudo]:
    with uow:
        case_data = ProgramRules.case_from_path(command.caminho)
        if case_data is None:
            return None
        case_name = ProgramRules.case_name_from_data(*case_data)
        if case_name is None:
            return None
        estudo = Estudo(
            command.caminho,
            command.nome,
            EstadoCaso.NAO_INICIADO,
        )
        if estudo is not None:
            uow.estudos.create(estudo)
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


def monitora(
    command: commands.MonitoraEstudo,
    monitor: MonitorCaso,
) -> bool:
    monitor.monitora()


def atualiza(
    command: commands.AtualizaEstudo, uow: AbstractEstudoUnitOfWork
) -> bool:
    with uow:
        estudo = uow.estudos.read(command.id_estudo)
        if estudo is not None:
            estudo.estado = command.estado
            uow.estudos.update(estudo)
        return estudo is not None


def sintetiza(
    command: commands.SintetizaEstudo, uow: AbstractEstudoUnitOfWork
) -> bool:
    with uow:
        estudo = uow.estudos.read(command.id_estudo)
    if estudo is not None:
        sintetizador = SintetizadorEstudo(estudo)
        return sintetizador.sintetiza_estudo()
    return False
