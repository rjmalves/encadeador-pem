from encadeador.adapters.orm import registry
from sqlalchemy import (  # type: ignore
    Table,
    Column,
    Integer,
    ForeignKey,
    String,
    Enum,
    DateTime,
)
from encadeador.modelos.runstatus import RunStatus


tabela_rodadas = Table(
    "rodadas",
    registry.metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("nome", String(255)),
    Column("estado", Enum(RunStatus)),
    Column("id_job", String(255), nullable=False),
    Column("caminho", String(255), nullable=False),
    Column("instante_inicio_execucao", DateTime),
    Column("instante_fim_execucao", DateTime),
    Column("numero_processadores", Integer),
    Column("nome_programa", String(255)),
    Column("versao_programa", String(255)),
    Column("id_caso", ForeignKey("casos.id")),
)
