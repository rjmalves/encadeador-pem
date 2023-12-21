from sqlalchemy import Table, Column, Integer, ForeignKey, String, Enum  # type: ignore
from encadeador.adapters.orm import registry
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.programa import Programa

tabela_casos = Table(
    "casos",
    registry.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("caminho", String(255), nullable=False),
    Column("nome", String(255)),
    Column("ano", Integer),
    Column("mes", Integer),
    Column("revisao", Integer),
    Column("programa", Enum(Programa)),
    Column("estado", Enum(EstadoCaso)),
    Column("id_estudo", ForeignKey("estudos.id")),
)
