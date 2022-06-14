from sqlalchemy import Table, Column, Integer, ForeignKey, String, Enum
from encadeador.adaptadores.orm import registry
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.programa import Programa

tabela_casos = Table(
    "casos",
    registry.metadata,
    Column("_id", Integer, primary_key=True, autoincrement=True),
    Column("_caminho", String(255), nullable=False),
    Column("_nome", String(255)),
    Column("_ano", Integer),
    Column("_mes", Integer),
    Column("_revisao", Integer),
    Column("_programa", Enum(Programa)),
    Column("_estado", Enum(EstadoCaso)),
    Column("_id_estudo", ForeignKey("estudos._id")),
)
