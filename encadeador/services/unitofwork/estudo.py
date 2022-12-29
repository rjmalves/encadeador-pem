from abc import ABC, abstractmethod
from sqlalchemy.orm import Session  # type: ignore
from typing import Dict, Type
from config import default_session_factory
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

    def __enter__(self) -> "AbstractEstudoUnitOfWork":
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


class SQLEstudoUnitOfWork(AbstractEstudoUnitOfWork):
    def __init__(self, session_factory=default_session_factory):
        self._session_factory = session_factory()

    def __enter__(self) -> "AbstractEstudoUnitOfWork":
        self._session: Session = self._session_factory()
        self._estudos = SQLEstudoRepository(self._session)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self._session.close()

    @property
    def estudos(self) -> AbstractEstudoRepository:
        return self._estudos

    def commit(self):
        self._commit()

    def _commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()


def factory(kind: str, *args, **kwargs) -> AbstractEstudoUnitOfWork:
    mappings: Dict[str, Type[AbstractEstudoUnitOfWork]] = {
        "SQL": SQLEstudoUnitOfWork,
        "JSON": JSONEstudoUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
