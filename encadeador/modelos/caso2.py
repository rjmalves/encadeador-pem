from abc import abstractmethod
from typing import List, Optional

from encadeador.modelos.programa import Programa
from encadeador.modelos.configuracoes import Configuracoes
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
    ) -> None:
        self._id = None
        self._caminho = str(_caminho)
        self._nome = _nome
        self._ano = _ano
        self._mes = _mes
        self._revisao = _revisao
        self._programa = _programa
        self._estado = _estado
        self._jobs: List[Job] = []

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
    @abstractmethod
    def numero_processadores(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def _constroi_nome_caso(self, ano: int, mes: int, revisao: int) -> str:
        raise NotImplementedError


class CasoNEWAVE(Caso):
    def __init__(self, caso: Caso):
        self._id = caso._id
        self._caminho = caso._caminho
        self._nome = caso._nome
        self._ano = caso._ano
        self._mes = caso._mes
        self._revisao = caso._revisao
        self._programa = caso._programa
        self._estado = caso._estado
        self._jobs = caso._jobs

    # Override
    @property
    def numero_processadores(self) -> int:
        # TODO - Ler o dger.dat e conferir as restrições de número
        # de processadores (séries forward)
        minimo = Configuracoes().processadores_minimos_newave
        maximo = Configuracoes().processadores_maximos_newave
        ajuste = Configuracoes().ajuste_processadores_newave
        num_proc = minimo
        if ajuste:
            num_proc = maximo
        return num_proc

    # Override
    @staticmethod
    def _constroi_nome_caso(ano: int, mes: int, revisao: int) -> str:
        return f"{Configuracoes().nome_estudo} - NW {mes}/{ano}"


class CasoDECOMP(Caso):
    def __init__(self, caso: Caso):
        self._id = caso._id
        self._caminho = caso._caminho
        self._nome = caso._nome
        self._ano = caso._ano
        self._mes = caso._mes
        self._revisao = caso._revisao
        self._programa = caso._programa
        self._estado = caso._estado
        self._jobs = caso._jobs

    # Override
    @property
    def numero_processadores(self) -> int:
        # TODO - Ler o dadger.rvX e conferir as restrições de número
        # de processadores (séries do 2º mês)
        minimo = Configuracoes().processadores_minimos_decomp
        maximo = Configuracoes().processadores_maximos_decomp
        ajuste = Configuracoes().ajuste_processadores_decomp
        num_proc = minimo
        if ajuste:
            num_proc = maximo
        return num_proc

    # Override
    @staticmethod
    def _constroi_nome_caso(ano: int, mes: int, revisao: int) -> str:
        return (
            f"{Configuracoes().nome_estudo} - DC" + f" {mes}/{ano} rv{revisao}"
        )


def factory(caso: Caso) -> Caso:
    if caso.programa == Programa.NEWAVE:
        return CasoNEWAVE(caso)
    elif caso.programa == Programa.DECOMP:
        return CasoDECOMP(caso)
    else:
        raise ValueError(f"Programa {caso.programa} não suportado")
