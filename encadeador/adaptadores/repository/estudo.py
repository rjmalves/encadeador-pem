from abc import ABC, abstractmethod
from sqlalchemy import select, update, delete
from sqlalchemy.orm import Session
from typing import List, Dict, Optional
from pathlib import Path
from os.path import exists
from os import makedirs
from json import dump, load

from encadeador.modelos.estudo2 import Estudo
from encadeador.modelos.estadoestudo import EstadoEstudo


from encadeador.adaptadores.repository.caso import JSONCasoRepository


class AbstractEstudoRepository(ABC):
    @abstractmethod
    def create(self, estudo: Estudo):
        raise NotImplementedError

    @abstractmethod
    def read(self, id: int) -> Optional[Estudo]:
        raise NotImplementedError

    @abstractmethod
    def update(self, estudo: Estudo):
        raise NotImplementedError

    @abstractmethod
    def delete(self, id: int):
        raise NotImplementedError

    @abstractmethod
    def list(self) -> List[Estudo]:
        raise NotImplementedError


class SQLEstudoRepository(ABC):
    def __init__(self, session: Session):
        self.__session = session

    def create(self, estudo: Estudo):
        self.__session.add(estudo)

    def read(self, id: int) -> Optional[Estudo]:
        statement = select(Estudo).filter_by(_id=id)
        try:
            j = self.__session.execute(statement).one()[0]
            return j
        except Exception:
            return None

    def update(self, estudo: Estudo):
        statement = (
            update(Estudo)
            .where(Estudo._id == estudo._id)
            .values(
                {
                    "_estado": estudo._estado,
                }
            )
        )
        return self.__session.execute(statement)

    def delete(self, id: int):
        statement = delete(Estudo).where(Estudo._id == id)
        return self.__session.execute(statement)

    def list(self) -> List[Estudo]:
        statement = select(Estudo)
        return [j[0] for j in self.__session.execute(statement).all()]


class JSONEstudoRepository(AbstractEstudoRepository):
    def __init__(self, path: str):
        self.__path = Path(path) / "estudos.json"
        self.__casos_repository = JSONCasoRepository(path)

    @staticmethod
    def __to_json(estudo: Estudo) -> dict:
        return {
            "_id": estudo.id,
            "_caminho": estudo.caminho,
            "_nome": estudo.nome,
            "_estado": estudo.estado.value,
        }

    @staticmethod
    def __from_json(estudo_data: dict) -> Estudo:
        estudo = Estudo(
            estudo_data["_caminho"],
            estudo_data["_nome"],
            EstadoEstudo.factory(estudo_data["_estado"]),
        )
        estudo._id = estudo_data["_id"]
        return estudo

    def __choose_id_for_new_estudo(self, existing: List[Estudo]) -> int:
        existing_ids = [j._id for j in existing]
        max_current_id = max(existing_ids) if len(existing_ids) > 0 else 0
        return max_current_id + 1

    def __create_directory_if_not_exists(self):
        if not exists(self.__path):
            if not exists(self.__path.parent):
                makedirs(self.__path.parent)
            with open(self.__path, "w") as file:
                file.write("[]")

    def __read_file(self) -> List[Estudo]:
        self.__create_directory_if_not_exists()
        with open(self.__path, "r") as file:
            estudos = [JSONEstudoRepository.__from_json(c) for c in load(file)]
            for e in estudos:
                e._casos = self.__casos_repository.list_by_estudo(e._id)
            return estudos

    def __write_file(self, estudos: List[Estudo]):
        self.__create_directory_if_not_exists()
        with open(self.__path, "w") as file:
            dump([JSONEstudoRepository.__to_json(c) for c in estudos], file)

    def create(self, estudo: Estudo):
        existing = self.__read_file()
        new_id = self.__choose_id_for_new_estudo(existing)
        estudo._id = new_id
        updated = existing + [estudo]
        self.__write_file(updated)

    def read(self, id: int) -> Optional[Estudo]:
        existing = self.__read_file()
        candidate = [c for c in existing if c._id == id]
        return candidate[0] if len(candidate) == 1 else None

    def update(self, estudo: Estudo):
        estudos = self.__read_file()
        candidates = list(filter(lambda c: c._id == estudo._id, estudos))
        if len(candidates) == 1:
            index = estudos.index(candidates[0])
            estudos[index] = estudo
            self.__write_file(estudos)

    def delete(self, id: int):
        existing = self.__read_file()
        deleted = [c for c in existing if c._id != id]
        return self.__write_file(deleted)

    def list(self) -> List[Estudo]:
        return self.__read_file()


def factory(kind: str, *args, **kwargs) -> AbstractEstudoRepository:
    mappings: Dict[str, AbstractEstudoRepository] = {
        "SQL": SQLEstudoRepository,
        "JSON": JSONEstudoRepository,
    }
    return mappings[kind](*args, **kwargs)
