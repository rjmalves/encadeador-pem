from sqlalchemy import Table, Column, Integer, String, Enum  # type: ignore

from encadeador.adapters.orm import registry
from encadeador.modelos.estadoestudo import EstadoEstudo


tabela_estudos = Table(
    "estudos",
    registry.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("caminho", String(255), nullable=False),
    Column("nome", String(255)),
    Column("estado", Enum(EstadoEstudo)),
)
