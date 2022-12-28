from abc import ABC, abstractmethod
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Dict

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.adapters.repository.estudo import (
    AbstractEstudoRepository,
    JSONEstudoRepository,
    SQLEstudoRepository,
)


class AbstractEstudoUnitOfWork(ABC):
    def __enter__(self) -> "AbstractEstudoUnitOfWork":
        return self

    def __exit__(self, *args):
        self.rollback()

    def commit(self):
        self._commit()

    @property
    @abstractmethod
    def estudos(self) -> AbstractEstudoRepository:
        raise NotImplementedError

    @abstractmethod
    def _commit(self):
        raise NotImplementedError

    @abstractmethod
    def rollback(self):
        raise NotImplementedError


class JSONEstudoUnitOfWork(AbstractEstudoUnitOfWork):
    def __init__(self, path: str = Configuracoes().caminho_base_estudo):
        self._path = path

    def __enter__(self) -> "JSONEstudoUnitOfWork":
        self._estudos = JSONEstudoRepository(self._path)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)

    @property
    def estudos(self) -> JSONEstudoRepository:
        return self._estudos

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


class SQLEstudoUnitOfWork(AbstractEstudoUnitOfWork):
    def __init__(
        self, session_factory: sessionmaker = DEFAULT_SESSION_FACTORY
    ):
        self._session_factory = session_factory

    def __enter__(self) -> "SQLEstudoUnitOfWork":
        self._session: Session = self._session_factory()
        self._estudos = SQLEstudoRepository(self._session)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self._session.close()

    @property
    def estudos(self) -> SQLEstudoRepository:
        return self._estudos

    def commit(self):
        self._commit()

    def _commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()


def factory(kind: str, *args, **kwargs) -> AbstractEstudoUnitOfWork:
    mappings: Dict[str, AbstractEstudoUnitOfWork] = {
        "SQL": SQLEstudoUnitOfWork,
        "JSON": JSONEstudoUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
