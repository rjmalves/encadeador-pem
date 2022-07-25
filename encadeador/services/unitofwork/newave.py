from abc import ABC, abstractmethod
from os import chdir, curdir, remove, listdir
import re
from typing import Optional, Dict
from os.path import isfile
from zipfile import ZipFile
from pathlib import Path


from encadeador.modelos.configuracoes import Configuracoes
from encadeador.adapters.repository.newave import (
    AbstractNewaveRepository,
    FSNewaveRepository,
)
from encadeador.utils.terminal import converte_codificacao


NEWAVE_OUT_ZIP_PATTERN = "saidas_.*zip"


class AbstractNewaveUnitOfWork(ABC):
    def __enter__(self) -> "AbstractNewaveUnitOfWork":
        return self

    def __exit__(self, *args):
        self.rollback()

    @abstractmethod
    def rollback(self):
        raise NotImplementedError

    @abstractmethod
    def extrai_cortes(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def deleta_cortes(self) -> bool:
        raise NotImplementedError

    @property
    @abstractmethod
    def newave(self) -> AbstractNewaveRepository:
        raise NotImplementedError


class FSNewaveUnitOfWork(AbstractNewaveUnitOfWork):
    def __init__(self, path: str):
        self._current_path = Path(curdir).resolve()
        self._newave_path = path

    def __enter__(self) -> "FSNewaveUnitOfWork":
        chdir(self._newave_path)
        converte_codificacao(
            self._newave_path, Configuracoes().script_converte_codificacao
        )
        self._newave = FSNewaveRepository(self._newave_path)
        return super().__enter__()

    def __exit__(self, *args):
        chdir(self._current_path)
        super().__exit__(*args)

    @property
    def newave(self) -> FSNewaveRepository:
        return self._newave

    def __out_zip_name(self) -> Optional[str]:
        out_zip = [
            r
            for r in listdir()
            if re.match(NEWAVE_OUT_ZIP_PATTERN, r) is not None
        ]
        if len(out_zip) == 1:
            return out_zip[0]
        else:
            return None

    def extrai_cortes(self) -> bool:
        cortes = [self.newave.arquivos.cortes, self.newave.arquivos.cortesh]
        zipname = self.__out_zip_name()
        if zipname is not None:
            with ZipFile(zipname, "r") as obj_zip:
                for a in cortes:
                    if not isfile(a):
                        obj_zip.extract(a)

    def deleta_cortes(self) -> bool:
        arqs = [self.newave.arquivos.cortes, self.newave.arquivos.cortesh]
        for a in arqs:
            if not isfile(a):
                return False
            remove(a)
        return True

    def rollback(self):
        pass


def factory(kind: str, *args, **kwargs) -> AbstractNewaveUnitOfWork:
    mappings: Dict[str, AbstractNewaveUnitOfWork] = {
        "FS": FSNewaveUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
