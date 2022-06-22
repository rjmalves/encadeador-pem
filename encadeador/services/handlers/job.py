from typing import Callable, Optional

from encadeador.controladores.gerenciadorfila2 import GerenciadorFila
from encadeador.modelos.estadojob import EstadoJob
from encadeador.services.unitofwork.job import AbstractJobUnitOfWork
from encadeador.modelos.job2 import Job
import encadeador.domain.commands as commands


# TODO - no futuro, quando toda a aplicação for
# orientada a eventos, o logging deve ser praticamente
# restrito aos handlers?


def submete(
    command: commands.SubmeteJob, uow: AbstractJobUnitOfWork
) -> Optional[Job]:
    with uow:
        job = Job(
            None,
            None,
            command.caminho,
            None,
            None,
            None,
            command.numero_processadores,
            EstadoJob.NAO_INICIADO,
            command.id_caso,
        )
        gerenciador = GerenciadorFila.factory(command.gerenciador, job)
        if gerenciador.submete():
            uow.jobs.create(job)
            uow.commit()
            return job


def monitora(
    command: commands.MonitoraJob, uow: AbstractJobUnitOfWork, event: Callable
):
    with uow:
        job = uow.jobs.read(command.id)
        if job is not None:
            gerenciador = GerenciadorFila.factory(command.gerenciador, job)
            estado = gerenciador.monitora()
            if estado != job.estado:
                job.atualiza(estado)
                uow.jobs.update(job)
                uow.commit()
                event(estado, job.estado)


def deleta(command: commands.DeletaJob, uow: AbstractJobUnitOfWork) -> bool:
    with uow:
        job = uow.jobs.read(command.id)
        if job is not None:
            gerenciador = GerenciadorFila.factory(command.gerenciador, job)
            return gerenciador.deleta()
        return False
