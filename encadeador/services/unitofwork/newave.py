from abc import ABC, abstractmethod
from os import chdir, curdir, remove, listdir
import re
from typing import Optional, Dict, Type
from os.path import isfile
from zipfile import ZipFile
from pathlib import Path
from shutil import move


from encadeador.adapters.repository.newave import (
    AbstractNewaveRepository,
    FSNewaveRepository,
)


NEWAVE_OUT_ZIP_PATTERN = "cortes_.*zip"


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

    def __enter__(self) -> "AbstractNewaveUnitOfWork":
        chdir(self._newave_path)
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

    async def extrai_cortes(self) -> bool:
        # Premissa: usa cortes por período a partir das versões
        #  - 28.16.2 do NEWAVE
        #  - 31.21 do DECOMP

        # Descobre o nome do arquivo de cortes
        # relativo ao 2º mês do estudo
        dger = await self.newave.get_dger()
        mes_inicio = dger.mes_inicio_estudo + 1
        arq_cortes = f"cortes-{str(mes_inicio).zfill(3)}.dat"

        cortes_extrair = {
            self.newave.arquivos.cortesh: self.newave.arquivos.cortesh,
            arq_cortes: "cortes.dat",
        }
        zipname = self.__out_zip_name()
        if zipname is not None:
            with ZipFile(zipname, "r") as obj_zip:
                for arq_zip, arq_extraido in cortes_extrair.items():
                    if arq_zip is None:
                        return False
                    if not isfile(arq_extraido):
                        obj_zip.extract(arq_zip)
                        move(arq_zip, arq_extraido)
            return True
        return False

    def deleta_cortes(self) -> bool:
        arqs = [self.newave.arquivos.cortes, self.newave.arquivos.cortesh]
        for a in arqs:
            if a is None:
                return False
            if not isfile(a):
                return False
            remove(a)
        return True

    def rollback(self):
        pass


def factory(kind: str, *args, **kwargs) -> AbstractNewaveUnitOfWork:
    mappings: Dict[str, Type[AbstractNewaveUnitOfWork]] = {
        "FS": FSNewaveUnitOfWork,
    }
    return mappings[kind](*args, **kwargs)
