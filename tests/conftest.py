import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, clear_mappers

from encadeador.adapters.orm import registry
from encadeador.adapters.orm.util import start_mappers


JSON_WRITING_FIRST_INDEX = 6


@pytest.fixture
def in_memory_sqlite_db():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    registry.metadata.create_all(engine)
    return engine


@pytest.fixture
def sqlite_session_factory(in_memory_sqlite_db):
    yield sessionmaker(bind=in_memory_sqlite_db)


@pytest.fixture
def mappers():
    start_mappers()
    yield
    clear_mappers()
