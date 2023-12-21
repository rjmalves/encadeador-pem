from abc import ABC, abstractmethod
from sqlalchemy.orm import Session  # type: ignore
from typing import Dict, Type
from config import default_session_factory
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.adapters.repository.rodada import (
    AbstractRodadaRepository,
    JSONRodadaRepository,
    SQLRodadaRepository,
)


class AbstractRodadaUnitOfWork(ABC):
    def __enter__(self) -> "AbstractRodadaUnitOfWork":
        return self

    def __exit__(self, *args):
        self.rollback()

    def commit(self):
        self._commit()

    @property
    @abstractmethod
    def rodadas(self) -> AbstractRodadaRepository:
        raise NotImplementedError

    @abstractmethod
    def _commit(self):
        raise NotImplementedError

    @abstractmethod
    def rollback(self):
        raise NotImplementedError


class JSONRodadaUnitOfWork(AbstractRodadaUnitOfWork):
    def __init__(self, path: str = Configuracoes().caminho_base_estudo):
        self._path = path

    def __enter__(self) -> "AbstractRodadaUnitOfWork":
        self._rodadas = JSONRodadaRepository(self._path)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)

    @property
    def rodadas(self) -> AbstractRodadaRepository:
        return self._rodadas

    def commit(self):
        self._commit()

    def _commit(self):
        pass

    def rollback(self):
        pass


class SQLRodadaUnitOfWork(AbstractRodadaUnitOfWork):
    def __init__(self, session_factory=default_session_factory):
        self._session_factory = session_factory()

    def __enter__(self) -> "AbstractRodadaUnitOfWork":
        self._session: Session = self._session_factory()
        self._rodadas = SQLRodadaRepository(self._session)
        return super().__enter__()

    def __exit__(self, *args):
        super().__exit__(*args)
        self._session.close()

    @property
    def rodadas(self) -> AbstractRodadaRepository:
        return self._rodadas

    def commit(self):
        self._commit()

    def _commit(self):
        self._session.commit()

    def rollback(self):
        self._session.rollback()


def factory(kind: str, *args, **kwargs) -> AbstractRodadaUnitOfWork:
    mappings: Dict[str, Type[AbstractRodadaUnitOfWork]] = {
        "SQL": SQLRodadaUnitOfWork,
        "JSON": JSONRodadaUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
