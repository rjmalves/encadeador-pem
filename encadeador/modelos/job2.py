from time import time
from typing import Optional

from encadeador.modelos.estadojob import EstadoJob


class Job:
    """
    Classe base para representar um job que é submetido e executado
    pelo gerenciador de filas de processos no cluster. É responsável
    apenas por guardar informações do estado do job e estatísticas de
    execução, mas não por enviar e interpretar comandos.
    """

    def __init__(
        self,
        _nome: str,
        _caminho: str,
        _instante_entrada_fila: float,
        _instante_inicio_execucao: float,
        _instante_saida_fila: float,
        _numero_processadores: int,
        _estado: EstadoJob,
        _id_caso: int,
    ):
        self._id = None
        self._nome = _nome
        self._caminho = str(_caminho)
        self._instante_entrada_fila = _instante_entrada_fila
        self._instante_inicio_execucao = _instante_inicio_execucao
        self._instante_saida_fila = _instante_saida_fila
        self._numero_processadores = _numero_processadores
        self._estado = _estado
        self._id_caso = _id_caso

    def atualiza(self, estado: EstadoJob):
        print(f"Job: {self.nome} - estado -> {estado.value}")
        self.estado = estado
        t = time()
        if self.estado == EstadoJob.ESPERANDO:
            self._instante_entrada_fila = t
        elif self.estado == EstadoJob.EXECUTANDO:
            self._instante_inicio_execucao = t
        elif self.estado in [EstadoJob.FINALIZADO, EstadoJob.ERRO]:
            self._instante_saida_fila = t

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def nome(self) -> str:
        return self._nome

    @property
    def caminho(self) -> str:
        return self._caminho

    @property
    def numero_processadores(self) -> int:
        return self._numero_processadores

    @numero_processadores.setter
    def numero_processadores(self, n: int):
        self._numero_processadores = n

    @property
    def estado(self) -> EstadoJob:
        return self._estado

    @estado.setter
    def estado(self, e: EstadoJob):
        self._estado = e

    @property
    def tempo_fila(self) -> float:
        if self._estado == EstadoJob.NAO_INICIADO:
            return 0.0
        elif self._estado == EstadoJob.ESPERANDO:
            return time() - self._instante_entrada_fila
        else:
            return self._instante_inicio_execucao - self._instante_entrada_fila

    @property
    def tempo_execucao(self) -> float:
        if self._estado in [EstadoJob.NAO_INICIADO, EstadoJob.ESPERANDO]:
            return 0.0
        elif self._estado in [EstadoJob.EXECUTANDO, EstadoJob.ERRO]:
            return time() - self._instante_entrada_fila
        else:
            return self._instante_saida_fila - self._instante_inicio_execucao
