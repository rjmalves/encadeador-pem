from encadeador.adapters.orm import registry
from sqlalchemy import create_engine
from encadeador.adapters.orm.util import start_mappers


def start_db(caminho: str):
    SQLITE_URL = f"sqlite:///{caminho}data.db"
    engine = create_engine(SQLITE_URL)
    registry.metadata.create_all(engine)
    start_mappers()
