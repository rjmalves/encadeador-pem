from sqlalchemy import Table, Column, Integer, String, Enum

from encadeador.adapters.orm import registry
from encadeador.modelos.estadoestudo import EstadoEstudo


tabela_estudos = Table(
    "estudos",
    registry.metadata,
    Column("_id", Integer, primary_key=True, autoincrement=True),
    Column("_caminho", String(255), nullable=False),
    Column("_nome", String(255)),
    Column("_estado", Enum(EstadoEstudo)),
)
