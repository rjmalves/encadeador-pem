from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Dict
from config import sqlite_url

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.adapters.repository.caso import (
    AbstractCasoRepository,
    JSONCasoRepository,
    SQLCasoRepository,
)


class AbstractCasoUnitOfWork(ABC):
    def __enter__(self) -> "AbstractCasoUnitOfWork":
        return self

    def __exit__(self, *args):
        self.rollback()

    def commit(self):
        self._commit()

    @property
    @abstractmethod
    def casos(self) -> AbstractCasoRepository:
        raise NotImplementedError

    @abstractmethod
    def _commit(self):
        raise NotImplementedError

    @abstractmethod
    def rollback(self):
        raise NotImplementedError


class JSONCasoUnitOfWork(AbstractCasoUnitOfWork):
    def __init__(self, path: str = Configuracoes().caminho_base_estudo):
        self._path = path

    def __enter__(self) -> "JSONCasoUnitOfWork":
        self._casos = JSONCasoRepository(self._path)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)

    @property
    def casos(self) -> JSONCasoRepository:
        return self._casos

    def commit(self):
        self._commit()

    def _commit(self):
        pass

    def rollback(self):
        pass


DEFAULT_SESSION_FACTORY = lambda _: sessionmaker(
    bind=create_engine(
        sqlite_url(),
    )
)


class SQLCasoUnitOfWork(AbstractCasoUnitOfWork):
    def __init__(self, session_factory=DEFAULT_SESSION_FACTORY):
        self._session_factory = session_factory()

    def __enter__(self) -> "SQLCasoUnitOfWork":
        self._session: Session = self._session_factory()
        self._casos = SQLCasoRepository(self._session)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self._session.close()

    @property
    def casos(self) -> SQLCasoRepository:
        return self._casos

    def commit(self):
        self._commit()

    def _commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()


def factory(kind: str, *args, **kwargs) -> AbstractCasoUnitOfWork:
    mappings: Dict[str, AbstractCasoUnitOfWork] = {
        "SQL": SQLCasoUnitOfWork,
        "JSON": SQLCasoUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
