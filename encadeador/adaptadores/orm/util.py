from sqlalchemy.orm import relationship
from encadeador.adaptadores.orm import registry
from encadeador.adaptadores.orm.job import tabela_jobs
from encadeador.adaptadores.orm.caso import tabela_casos
from encadeador.adaptadores.orm.estudo import tabela_estudos

from encadeador.modelos.job2 import Job
from encadeador.modelos.caso2 import Caso
from encadeador.modelos.estudo2 import Estudo


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
    estudo_mapper = registry.map_imperatively(
        Estudo,
        tabela_estudos,
        properties={
            "_casos": relationship(
                caso_mapper,
                collection_class=list,
            )
        },
    )
