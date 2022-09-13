from datetime import datetime
from typing import List, Optional

from encadeador.modelos.programa import Programa
from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.job import Job
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.utils.log import Log


class Caso:
    """
    Classe base que representa todos os casos executados no
    estudo encadeado. É responsável por armazenar informações
    associadas ao caso. Não lida com a sua execução, pré ou pós
    processamento dos decks.
    """

    def __init__(
        self,
        _caminho: str,
        _nome: str,
        _ano: int,
        _mes: int,
        _revisao: int,
        _programa: Programa,
        _estado: EstadoCaso,
        _id_estudo: int,
    ) -> None:
        self._id = None
        self._caminho = str(_caminho)
        self._nome = _nome
        self._ano = _ano
        self._mes = _mes
        self._revisao = _revisao
        self._programa = _programa
        self._estado = _estado
        self._id_estudo = _id_estudo
        self._jobs: List[Job] = []

    def __eq__(self, o: object):
        if not isinstance(o, Caso):
            return False
        return all(
            [
                self.id == o.id,
                self.caminho == o.caminho,
                self.nome == o.nome,
                self.ano == o.ano,
                self.mes == o.mes,
                self.revisao == o.revisao,
                self.programa == o.programa,
                self.estado == o.estado,
                self._id_estudo == o._id_estudo,
            ]
        )

    def __ge__(self, o: object):
        if not isinstance(o, Caso):
            return False
        return datetime(self.ano, self.mes, 1) >= datetime(o.ano, o.mes, 1)

    def adiciona_job(self, job: Job):
        if len(self._jobs) > 0:
            if self._jobs[-1].estado != EstadoJob.FINALIZADO:
                self._jobs[-1] = job
        else:
            self._jobs.append(job)

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def caminho(self) -> str:
        return self._caminho

    @caminho.setter
    def caminho(self, c: str):
        self._caminho = c

    @property
    def nome(self) -> str:
        return self._nome

    @property
    def tempo_fila(self) -> float:
        return sum([j.tempo_fila for j in self._jobs])

    @property
    def tempo_execucao(self) -> float:
        return sum([j.tempo_execucao for j in self._jobs])

    @property
    def ano(self) -> int:
        return self._ano

    @property
    def mes(self) -> int:
        return self._mes

    @property
    def revisao(self) -> int:
        return self._revisao

    @property
    def numero_flexibilizacoes(self) -> int:
        return len(self._jobs) - 1

    @property
    def programa(self) -> Programa:
        return self._programa

    @property
    def estado(self) -> EstadoCaso:
        return self._estado

    @estado.setter
    def estado(self, e: EstadoCaso):
        Log.log().debug(f"Caso: {self.nome} - estado -> {e.value}")
        self._estado = e

    @property
    def jobs(self) -> List[Job]:
        return self._jobs

    @property
    def id_estudo(self) -> int:
        return self._id_estudo
