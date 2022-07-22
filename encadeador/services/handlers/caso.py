from typing import Callable, Optional
from encadeador.controladores.monitorjob2 import MonitorJob
from encadeador.controladores.preparadorcaso2 import PreparadorCaso

from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.services.unitofwork.caso import AbstractCasoUnitOfWork
from encadeador.modelos.caso2 import Caso
import encadeador.domain.commands as commands
from encadeador.domain.naming import NamingRules
from encadeador.services.unitofwork.job import AbstractJobUnitOfWork


# TODO - no futuro, quando toda a aplicação for
# orientada a eventos, o logging deve ser praticamente
# restrito aos handlers?


def cria(
    command: commands.CriaCaso, uow: AbstractCasoUnitOfWork
) -> Optional[Caso]:
    with uow:
        case_data = NamingRules.case_from_path(command.caminho)
        if case_data is None:
            return None
        case_name = NamingRules.case_name_from_data(*case_data)
        if case_name is None:
            return None
        caso = Caso(
            command.caminho,
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
            uow.commit()
        return caso


def inicializa(
    command: commands.InicializaCaso,
    uow: AbstractCasoUnitOfWork,
) -> Optional[Caso]:
    with uow:
        caso = uow.casos.read(command.id_caso)
        if caso is not None:
            caso.estado = EstadoCaso.INICIADO
            uow.commit()
        return caso


def prepara(
    command: commands.PreparaCaso, uow: AbstractCasoUnitOfWork
) -> Optional[Caso]:
    with uow:
        # Lista os casos anteriores
        caso = uow.casos.read(command.id_caso)
        casos_anteriores = [
            c for c in uow.casos.list_by_estudo(caso.id_estudo) if c < caso
        ]
        preparador = PreparadorCaso.factory(caso, casos_anteriores)
        sucesso_prepara = preparador.prepara()
        sucesso_encadeia = preparador.encadeia()
        sucesso_regras = preparador.aplica_regras_operacao_reservatorios(
            command.regras_reservatorios
        )
        return all([sucesso_prepara, sucesso_encadeia, sucesso_regras])


def submete(
    command: commands.SubmeteCaso,
    caso_uow: AbstractCasoUnitOfWork,
    monitor: MonitorJob,
) -> bool:
    with caso_uow:
        # Extrai o caso
        caso = caso_uow.casos.read(command.id_caso)
        if caso is None:
            return None
        return monitor.submete(
            caso.caminho,
            command.numero_processadores,
            command.id_caso,
            command.gerenciador,
        )
