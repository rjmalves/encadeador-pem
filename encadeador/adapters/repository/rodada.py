from abc import ABC, abstractmethod
from sqlalchemy import select, update, delete  # type: ignore
from sqlalchemy.orm import Session  # type: ignore
from typing import List, Dict, Optional, Type
from pathlib import Path
from os.path import exists
from os import makedirs
from json import dump, load
from datetime import datetime

from encadeador.modelos.rodada import Rodada
from encadeador.modelos.runstatus import RunStatus


class AbstractRodadaRepository(ABC):
    @abstractmethod
    def create(self, rodada: Rodada):
        raise NotImplementedError

    @abstractmethod
    def read(self, id: int) -> Optional[Rodada]:
        raise NotImplementedError

    @abstractmethod
    def update(self, rodada: Rodada):
        raise NotImplementedError

    @abstractmethod
    def delete(self, id: int):
        raise NotImplementedError

    @abstractmethod
    def list(self) -> List[Rodada]:
        raise NotImplementedError

    @abstractmethod
    def list_by_caso(self, id_caso: int) -> List[Rodada]:
        raise NotImplementedError


class SQLRodadaRepository(AbstractRodadaRepository):
    def __init__(self, session: Session):
        self.__session = session

    def create(self, rodada: Rodada):
        return self.__session.add(rodada)

    def read(self, id: int) -> Optional[Rodada]:
        statement = select(Rodada).filter_by(id=id)
        try:
            j = self.__session.execute(statement).one()[0]
            return j
        except Exception:
            return None

    def update(self, rodada: Rodada):
        statement = (
            update(Rodada)
            .where(Rodada.id == rodada.id)
            .values(
                {
                    "instante_inicio_execucao": rodada.instante_inicio_execucao,
                    "instante_fim_execucao": rodada.instante_fim_execucao,
                    "estado": rodada.estado,
                }
            )
        )
        return self.__session.execute(statement)

    def delete(self, id: int):
        statement = delete(Rodada).where(Rodada.id == id)
        return self.__session.execute(statement)

    def list(self) -> List[Rodada]:
        statement = select(Rodada)
        return [j[0] for j in self.__session.execute(statement).all()]

    def list_by_caso(self, id_caso: int) -> List[Rodada]:
        statement = select(Rodada).where(Rodada.id_caso == id_caso)
        return [j[0] for j in self.__session.execute(statement).all()]


class JSONRodadaRepository(AbstractRodadaRepository):
    def __init__(self, path: str):
        self.__path = Path(path) / "rodadas.json"

    @staticmethod
    def __to_json(rodada: Rodada) -> dict:
        fim_exec = None
        if rodada.instante_fim_execucao is not None:
            fim_exec = rodada.instante_fim_execucao.isoformat()
        return {
            "id": rodada.id,
            "nome": rodada.nome,
            "estado": rodada.estado.value,
            "id_job": rodada.id_job,
            "caminho": rodada.caminho,
            "instante_inicio_execucao": rodada.instante_inicio_execucao.isoformat(),
            "instante_fim_execucao": fim_exec,
            "numero_processadores": rodada.numero_processadores,
            "nome_programa": rodada.nome_programa,
            "versao_programa": rodada.versao_programa,
            "id_caso": rodada.id_caso,
        }

    @staticmethod
    def __from_json(rodada_data: dict) -> Rodada:
        rodada = Rodada(
            rodada_data["nome"],
            RunStatus.factory(rodada_data["estado"]),
            rodada_data["id_job"],
            rodada_data["caminho"],
            datetime.fromisoformat(rodada_data["instante_inicio_execucao"]),
            datetime.fromisoformat(rodada_data["instante_fim_execucao"]),
            rodada_data["numero_processadores"],
            rodada_data["nome_programa"],
            rodada_data["versao_programa"],
            rodada_data["id_caso"],
        )
        rodada.id = rodada_data["id"]
        return rodada

    def __choose_id_for_new_rodada(self, existing: List[Rodada]) -> int:
        existing_ids = [j.id for j in existing]
        max_current_id = max(existing_ids) if len(existing_ids) > 0 else 0
        return max_current_id + 1

    def __create_directory_if_not_exists(self):
        if not exists(self.__path):
            if not exists(self.__path.parent):
                makedirs(self.__path.parent)
            with open(self.__path, "w") as file:
                file.write("[]")

    def __read_file(self) -> List[Rodada]:
        self.__create_directory_if_not_exists()
        with open(self.__path, "r") as file:
            return [JSONRodadaRepository.__from_json(j) for j in load(file)]

    def __write_file(self, jobs: List[Rodada]):
        self.__create_directory_if_not_exists()
        with open(self.__path, "w") as file:
            dump([JSONRodadaRepository.__to_json(j) for j in jobs], file)

    def create(self, rodada: Rodada):
        existing = self.__read_file()
        new_id = self.__choose_id_for_new_rodada(existing)
        rodada.id = new_id
        updated = existing + [rodada]
        self.__write_file(updated)

    def read(self, id: int) -> Optional[Rodada]:
        existing = self.__read_file()
        candidate = [j for j in existing if j.id == id]
        return candidate[0] if len(candidate) == 1 else None

    def update(self, rodada: Rodada):
        rodadas = self.__read_file()
        candidates = list(filter(lambda j: j.id == rodada.id, rodadas))
        if len(candidates) == 1:
            index = rodadas.index(candidates[0])
            rodadas[index] = rodada
            self.__write_file(rodadas)

    def delete(self, id: int):
        existing = self.__read_file()
        deleted = [j for j in existing if j.id != id]
        return self.__write_file(deleted)

    def list(self) -> List[Rodada]:
        return self.__read_file()

    def list_by_caso(self, id_caso: int) -> List[Rodada]:
        return [j for j in self.__read_file() if j.id_caso == id_caso]


def factory(kind: str, *args, **kwargs) -> AbstractRodadaRepository:
    mappings: Dict[str, Type[AbstractRodadaRepository]] = {
        "SQL": SQLRodadaRepository,
        "JSON": JSONRodadaRepository,
    }
    return mappings[kind](*args, **kwargs)
