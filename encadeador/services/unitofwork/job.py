from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Dict

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.adapters.repository.job import (
    AbstractJobRepository,
    JSONJobRepository,
    SQLJobRepository,
)


class AbstractJobUnitOfWork(ABC):
    def __enter__(self) -> "AbstractJobUnitOfWork":
        return self

    def __exit__(self, *args):
        self.rollback()

    def commit(self):
        self._commit()

    @property
    @abstractmethod
    def jobs(self) -> AbstractJobRepository:
        raise NotImplementedError

    @abstractmethod
    def _commit(self):
        raise NotImplementedError

    @abstractmethod
    def rollback(self):
        raise NotImplementedError


class JSONJobUnitOfWork(AbstractJobUnitOfWork):
    def __init__(self, path: str):
        self._path = path

    def __enter__(self) -> "JSONJobUnitOfWork":
        self._jobs = JSONJobRepository(self._path)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)

    @property
    def jobs(self) -> JSONJobRepository:
        return self._jobs

    def commit(self):
        self._commit()

    def _commit(self):
        pass

    def rollback(self):
        pass


DEFAULT_SESSION_FACTORY = sessionmaker(
    bind=create_engine(
        f"sqlite:///{Configuracoes().caminho_base_estudo}",
    )
)


class SQLJobUnitOfWork(AbstractJobUnitOfWork):
    def __init__(
        self, session_factory: sessionmaker = DEFAULT_SESSION_FACTORY
    ):
        self._session_factory = session_factory

    def __enter__(self) -> "SQLJobUnitOfWork":
        self._session: Session = self._session_factory()
        self._jobs = SQLJobRepository(self._session)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self._session.close()

    @property
    def jobs(self) -> SQLJobRepository:
        return self._jobs

    def commit(self):
        self._commit()

    def _commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()


def factory(kind: str, *args, **kwargs) -> AbstractJobUnitOfWork:
    mappings: Dict[str, AbstractJobUnitOfWork] = {
        "SQL": SQLJobUnitOfWork,
        "JSON": JSONJobUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
