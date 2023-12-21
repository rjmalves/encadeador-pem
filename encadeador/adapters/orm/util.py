from sqlalchemy.orm import relationship  # type: ignore
from encadeador.adapters.orm import registry
from encadeador.adapters.orm.rodada import tabela_rodadas
from encadeador.adapters.orm.caso import tabela_casos
from encadeador.adapters.orm.estudo import tabela_estudos

from encadeador.modelos.rodada import Rodada
from encadeador.modelos.caso import Caso
from encadeador.modelos.estudo import Estudo


def start_mappers():
    rodada_mapper = registry.map_imperatively(Rodada, tabela_rodadas)
    caso_mapper = registry.map_imperatively(
        Caso,
        tabela_casos,
        properties={
            "rodadas": relationship(
                rodada_mapper,
                collection_class=list,
            )
        },
    )
    estudo_mapper = registry.map_imperatively(  # noqa
        Estudo,
        tabela_estudos,
        properties={
            "casos": relationship(
                caso_mapper,
                collection_class=list,
            )
        },
    )
