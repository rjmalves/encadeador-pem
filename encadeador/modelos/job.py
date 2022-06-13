from time import time
from typing import Dict, Any

from encadeador.modelos.dadosjob import DadosJob
from encadeador.modelos.estadojob import EstadoJob
from encadeador.utils.log import Log


class Job:
    """
    Classe base para representar um job que é submetido e executado
    pelo gerenciador de filas de processos no cluster. É responsável
    apenas por guardar informações do estado do job e estatísticas de
    execução, mas não por enviar e interpretar comandos.
    Junto com a classe DadosJob, implementa o padrão Proxy, na forma de
    proteção de acesso (protection proxy).
    """

    def __init__(
        self, dados: DadosJob, estado: EstadoJob = EstadoJob.NAO_INICIADO
    ):
        self._dados = dados
        self._estado = estado

    @staticmethod
    def from_json(json_dict: Dict[str, Any]):
        dados = DadosJob.from_json(json_dict["_dados"])
        estado = EstadoJob.factory(json_dict["_estado"])
        job = Job(dados, estado)
        return job

    def to_json(self) -> Dict[str, Any]:
        return {
            "_dados": self._dados.to_json(),
            "_estado": str(self._estado.value),
        }

    def atualiza(self, estado: EstadoJob):
        Log.log().debug(f"Job: {self._dados.nome} - estado -> {estado.value}")
        self.estado = estado
        t = time()
        if self.estado == EstadoJob.ESPERANDO:
            self._dados.instante_entrada_fila = t
        elif self.estado == EstadoJob.EXECUTANDO:
            self._dados.instante_inicio_execucao = t
        elif self.estado in [EstadoJob.FINALIZADO, EstadoJob.ERRO]:
            self._dados.instante_saida_fila = t

    @property
    def id(self) -> str:
        return self._dados.id

    @id.setter
    def id(self, i: str):
        self._dados.id = i

    @property
    def nome(self) -> str:
        return self._dados.nome

    @property
    def caminho(self) -> str:
        return self._dados.caminho

    @property
    def numero_processadores(self) -> int:
        return self._dados.numero_processadores

    @numero_processadores.setter
    def numero_processadores(self, n: int):
        self._dados.numero_processadores = n

    @property
    def estado(self) -> EstadoJob:
        return self._estado

    @estado.setter
    def estado(self, e: EstadoJob):
        self._estado = e

    @property
    def tempo_fila(self) -> float:
        if self.estado == EstadoJob.NAO_INICIADO:
            return 0.0
        elif self.estado == EstadoJob.ESPERANDO:
            return time() - self._dados.instante_entrada_fila
        else:
            return (
                self._dados.instante_inicio_execucao
                - self._dados.instante_entrada_fila
            )

    @property
    def tempo_execucao(self) -> float:
        if self.estado in [EstadoJob.NAO_INICIADO, EstadoJob.ESPERANDO]:
            return 0.0
        elif self.estado in [EstadoJob.EXECUTANDO, EstadoJob.ERRO]:
            return time() - self._dados.instante_entrada_fila
        else:
            return (
                self._dados.instante_saida_fila
                - self._dados.instante_inicio_execucao
            )
