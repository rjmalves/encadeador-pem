from abc import ABC, abstractmethod
from sqlalchemy import select, update, delete
from sqlalchemy.orm import Session
from typing import List, Dict, Optional
from pathlib import Path
from os.path import exists
from os import makedirs
from json import dump, load
from datetime import datetime

from encadeador.modelos.job2 import Job
from encadeador.modelos.estadojob import EstadoJob


class AbstractJobRepository(ABC):
    @abstractmethod
    def create(self, job: Job):
        raise NotImplementedError

    @abstractmethod
    def read(self, id: int) -> Optional[Job]:
        raise NotImplementedError

    @abstractmethod
    def update(self, job: Job):
        raise NotImplementedError

    @abstractmethod
    def delete(self, id: int):
        raise NotImplementedError

    @abstractmethod
    def list(self) -> List[Job]:
        raise NotImplementedError

    @abstractmethod
    def list_by_caso(self, id_caso: int) -> List[Job]:
        raise NotImplementedError


class SQLJobRepository(AbstractJobRepository):
    def __init__(self, session: Session):
        self.__session = session

    def create(self, job: Job):
        return self.__session.add(job)

    def read(self, id: int) -> Optional[Job]:
        statement = select(Job).filter_by(_id=id)
        try:
            j = self.__session.execute(statement).one()[0]
            return j
        except Exception:
            return None

    def update(self, job: Job):
        statement = (
            update(Job)
            .where(Job._id == job._id)
            .values(
                {
                    "_instante_entrada_fila": job._instante_entrada_fila,
                    "_instante_inicio_execucao": job._instante_inicio_execucao,
                    "_instante_saida_fila": job._instante_saida_fila,
                    "_estado": job.estado,
                }
            )
        )
        return self.__session.execute(statement)

    def delete(self, id: int):
        statement = delete(Job).where(Job._id == id)
        return self.__session.execute(statement)

    def list(self) -> List[Job]:
        statement = select(Job)
        return [j[0] for j in self.__session.execute(statement).all()]

    def list_by_caso(self, id_caso: int) -> List[Job]:
        statement = select(Job).where(Job._id_caso == id_caso)
        return [j[0] for j in self.__session.execute(statement).all()]


class JSONJobRepository(AbstractJobRepository):
    def __init__(self, path: str):
        self.__path = Path(path) / "jobs.json"

    @staticmethod
    def __to_json(job: Job) -> dict:
        return {
            "_id": job.id,
            "_nome": job.nome,
            "_caminho": job.caminho,
            "_instante_entrada_fila": job._instante_entrada_fila.isoformat(),
            "_instante_inicio_execucao": job._instante_inicio_execucao.isoformat(),
            "_instante_saida_fila": job._instante_saida_fila.isoformat(),
            "_numero_processadores": job.numero_processadores,
            "_estado": job.estado.value,
            "_id_caso": job._id_caso,
        }

    @staticmethod
    def __from_json(job_data: dict) -> Job:
        job = Job(
            job_data["_nome"],
            job_data["_caminho"],
            datetime.fromisoformat(job_data["_instante_entrada_fila"]),
            datetime.fromisoformat(job_data["_instante_inicio_execucao"]),
            datetime.fromisoformat(job_data["_instante_saida_fila"]),
            job_data["_numero_processadores"],
            EstadoJob.factory(job_data["_estado"]),
            job_data["_id_caso"],
        )
        job._id = job_data["_id"]
        return job

    def __choose_id_for_new_job(self, existing: List[Job]) -> int:
        existing_ids = [j._id for j in existing]
        max_current_id = max(existing_ids) if len(existing_ids) > 0 else 0
        return max_current_id + 1

    def __create_directory_if_not_exists(self):
        if not exists(self.__path):
            if not exists(self.__path.parent):
                makedirs(self.__path.parent)
            with open(self.__path, "w") as file:
                file.write("[]")

    def __read_file(self) -> List[Job]:
        self.__create_directory_if_not_exists()
        with open(self.__path, "r") as file:
            return [JSONJobRepository.__from_json(j) for j in load(file)]

    def __write_file(self, jobs: List[Job]):
        self.__create_directory_if_not_exists()
        with open(self.__path, "w") as file:
            dump([JSONJobRepository.__to_json(j) for j in jobs], file)

    def create(self, job: Job):
        existing = self.__read_file()
        new_id = self.__choose_id_for_new_job(existing)
        job._id = new_id
        updated = existing + [job]
        self.__write_file(updated)

    def read(self, id: int) -> Optional[Job]:
        existing = self.__read_file()
        candidate = [j for j in existing if j._id == id]
        return candidate[0] if len(candidate) == 1 else None

    def update(self, job: Job):
        jobs = self.__read_file()
        candidates = list(filter(lambda j: j._id == job._id, jobs))
        if len(candidates) == 1:
            index = jobs.index(candidates[0])
            jobs[index] = job
            self.__write_file(jobs)

    def delete(self, id: int):
        existing = self.__read_file()
        deleted = [j for j in existing if j._id != id]
        return self.__write_file(deleted)

    def list(self) -> List[Job]:
        return self.__read_file()

    def list_by_caso(self, id_caso: int) -> List[Job]:
        return [j for j in self.__read_file() if j._id_caso == id_caso]


def factory(kind: str, *args, **kwargs) -> AbstractJobRepository:
    mappings: Dict[str, AbstractJobRepository] = {
        "SQL": SQLJobRepository,
        "JSON": JSONJobRepository,
    }
    return mappings[kind](*args, **kwargs)
