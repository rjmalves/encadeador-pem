from encadeador.adaptadores.orm import registry
from sqlalchemy import Table, Column, Integer, ForeignKey, String, Float, Enum
from encadeador.modelos.estadojob import EstadoJob


tabela_jobs = Table(
    "jobs",
    registry.metadata,
    Column("_id", Integer, primary_key=True, autoincrement=True),
    Column("_nome", String(255)),
    Column("_caminho", String(255), nullable=False),
    Column("_instante_entrada_fila", Float),
    Column("_instante_inicio_execucao", Float),
    Column("_instante_saida_fila", Float),
    Column("_numero_processadores", Integer),
    Column("_estado", Enum(EstadoJob)),
    Column("_id_caso", ForeignKey("casos._id")),
)
