from sqlalchemy.orm import relationship  # type: ignore
from encadeador.adapters.orm import registry
from encadeador.adapters.orm.job import tabela_jobs
from encadeador.adapters.orm.caso import tabela_casos
from encadeador.adapters.orm.estudo import tabela_estudos

from encadeador.modelos.job import Job
from encadeador.modelos.caso import Caso
from encadeador.modelos.estudo import Estudo


def start_mappers():
    job_mapper = registry.map_imperatively(Job, tabela_jobs)
    caso_mapper = registry.map_imperatively(
        Caso,
        tabela_casos,
        properties={
            "_jobs": relationship(
                job_mapper,
                collection_class=list,
            )
        },
    )
    estudo_mapper = registry.map_imperatively(  # noqa
        Estudo,
        tabela_estudos,
        properties={
            "_casos": relationship(
                caso_mapper,
                collection_class=list,
            )
        },
    )
