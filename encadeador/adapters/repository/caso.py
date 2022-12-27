from abc import ABC, abstractmethod
from sqlalchemy import select, update, delete  # type: ignore
from sqlalchemy.orm import Session  # type: ignore
from typing import List, Dict, Optional
from pathlib import Path
from os.path import exists
from os import makedirs
from json import dump, load

from encadeador.modelos.caso import Caso
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.programa import Programa


from encadeador.adapters.repository.rodada import JSONRodadaRepository


class AbstractCasoRepository(ABC):
    @abstractmethod
    def create(self, caso: Caso):
        raise NotImplementedError

    @abstractmethod
    def read(self, id: int) -> Optional[Caso]:
        raise NotImplementedError

    @abstractmethod
    def update(self, caso: Caso):
        raise NotImplementedError

    @abstractmethod
    def delete(self, id: int):
        raise NotImplementedError

    @abstractmethod
    def list(self) -> List[Caso]:
        raise NotImplementedError

    @abstractmethod
    def list_by_estudo(self, id_estudo: int) -> List[Caso]:
        raise NotImplementedError


class SQLCasoRepository(ABC):
    def __init__(self, session: Session):
        self.__session = session

    def create(self, caso: Caso):
        self.__session.add(caso)

    def read(self, id: int) -> Optional[Caso]:
        statement = select(Caso).filter_by(id=id)
        try:
            j = self.__session.execute(statement).one()[0]
            return j
        except Exception:
            return None

    def update(self, caso: Caso):
        statement = (
            update(Caso)
            .where(Caso.id == caso.id)
            .values(
                {
                    "estado": caso.estado,
                }
            )
        )
        return self.__session.execute(statement)

    def delete(self, id: int):
        statement = delete(Caso).where(Caso.id == id)
        return self.__session.execute(statement)

    def list(self) -> List[Caso]:
        statement = select(Caso)
        return [j[0] for j in self.__session.execute(statement).all()]

    def list_by_estudo(self, id_estudo: int) -> List[Caso]:
        statement = select(Caso).where(Caso.id_estudo == id_estudo)
        return [j[0] for j in self.__session.execute(statement).all()]


class JSONCasoRepository(AbstractCasoRepository):
    def __init__(self, path: str):
        self.__path = Path(path) / "casos.json"
        self.__rodadas_repository = JSONRodadaRepository(path)

    @staticmethod
    def __to_json(caso: Caso) -> dict:
        return {
            "id": caso.id,
            "caminho": caso.caminho,
            "nome": caso.nome,
            "ano": caso.ano,
            "mes": caso.mes,
            "revisao": caso.revisao,
            "programa": caso.programa.value,
            "estado": caso.estado.value,
            "id_estudo": caso.id_estudo,
        }

    @staticmethod
    def __from_json(caso_data: dict) -> Caso:
        caso = Caso(
            caso_data["caminho"],
            caso_data["nome"],
            caso_data["ano"],
            caso_data["mes"],
            caso_data["revisao"],
            Programa.factory(caso_data["programa"]),
            EstadoCaso.factory(caso_data["estado"]),
            caso_data["id_estudo"],
        )
        caso.id = caso_data["id"]
        return caso

    def __choose_id_for_new_caso(self, existing: List[Caso]) -> int:
        existing_ids = [j.id for j in existing]
        max_current_id = max(existing_ids) if len(existing_ids) > 0 else 0
        return max_current_id + 1

    def __create_directory_if_not_exists(self):
        if not exists(self.__path):
            if not exists(self.__path.parent):
                makedirs(self.__path.parent)
            with open(self.__path, "w") as file:
                file.write("[]")

    def __read_file(self) -> List[Caso]:
        self.__create_directory_if_not_exists()
        with open(self.__path, "r") as file:
            casos = [JSONCasoRepository.__from_json(c) for c in load(file)]
            for c in casos:
                c.rodadas = self.__rodadas_repository.list_by_caso(c.id)
            return casos

    def __write_file(self, casos: List[Caso]):
        self.__create_directory_if_not_exists()
        with open(self.__path, "w") as file:
            dump([JSONCasoRepository.__to_json(c) for c in casos], file)

    def create(self, caso: Caso):
        existing = self.__read_file()
        new_id = self.__choose_id_for_new_caso(existing)
        caso.id = new_id
        updated = existing + [caso]
        self.__write_file(updated)

    def read(self, id: int) -> Optional[Caso]:
        existing = self.__read_file()
        candidate = [c for c in existing if c.id == id]
        return candidate[0] if len(candidate) == 1 else None

    def update(self, caso: Caso):
        casos = self.__read_file()
        candidates = list(filter(lambda c: c.id == caso.id, casos))
        if len(candidates) == 1:
            index = casos.index(candidates[0])
            casos[index] = caso
            self.__write_file(casos)

    def delete(self, id: int):
        existing = self.__read_file()
        deleted = [c for c in existing if c.id != id]
        return self.__write_file(deleted)

    def list(self) -> List[Caso]:
        return self.__read_file()

    def list_by_estudo(self, id_estudo: int) -> List[Caso]:
        return [j for j in self.__read_file() if j.id_estudo == id_estudo]


def factory(kind: str, *args, **kwargs) -> AbstractCasoRepository:
    mappings: Dict[str, AbstractCasoRepository] = {
        "SQL": SQLCasoRepository,
        "JSON": JSONCasoRepository,
    }
    return mappings[kind](*args, **kwargs)
