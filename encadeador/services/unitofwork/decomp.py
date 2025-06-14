from abc import ABC, abstractmethod
from os import chdir, curdir
from typing import Dict, Type
from pathlib import Path

from encadeador.adapters.repository.decomp import (
    AbstractDecompRepository,
    FSDecompRepository,
)


class AbstractDecompUnitOfWork(ABC):
    def __enter__(self) -> "AbstractDecompUnitOfWork":
        return self

    def __exit__(self, *args):
        self.rollback()

    @abstractmethod
    def rollback(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def decomp(self) -> AbstractDecompRepository:
        raise NotImplementedError


class FSDecompUnitOfWork(AbstractDecompUnitOfWork):
    def __init__(self, path: str):
        self._current_path = Path(curdir).resolve()
        self._decomp_path = path

    def __enter__(self) -> "AbstractDecompUnitOfWork":
        chdir(self._decomp_path)
        self._decomp = FSDecompRepository(self._decomp_path)
        return super().__enter__()

    def __exit__(self, *args):
        chdir(self._current_path)
        super().__exit__(*args)

    @property
    def decomp(self) -> FSDecompRepository:
        return self._decomp

    def rollback(self):
        pass


def factory(kind: str, *args, **kwargs) -> AbstractDecompUnitOfWork:
    mappings: Dict[str, Type[AbstractDecompUnitOfWork]] = {
        "FS": FSDecompUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
