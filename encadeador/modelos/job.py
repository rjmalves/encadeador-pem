import pandas as pd  # type: ignore

from encadeador.modelos.dadosjob import DadosJob
from encadeador.modelos.estadojob import EstadoJob


class Job:
    """
    Classe base para representar um job que é submetido e executado
    pelo gerenciador de filas de processos no cluster. É responsável
    apenas por guardar informações do estado do job e estatísticas de
    execução, mas não por enviar e interpretar comandos.
    Junto com a classe DadosJob, implementa o padrão Proxy, na forma de
    proteção de acesso (protection proxy).
    """

    def __init__(self,
                 dados: DadosJob,
                 estado: EstadoJob = EstadoJob.NAO_INICIADO):
        self._dados = dados
        self._estado = estado

    @staticmethod
    def recupera_dados_linha(linha: pd.Series):
        dados = DadosJob.recupera_dados_linha(linha)
        estado = EstadoJob.factory(linha["Estado"])
        job = Job(dados, estado)
        return job

    def resume_dados(self) -> pd.Series:
        dados = self._dados.resume_dados()
        dados["Estado"] = str(self._estado.value)
        return dados

    def atualiza(self, estado: EstadoJob):
        pass

    @property
    def id(self) -> str:
        return self._dados.id

    @property
    def nome(self) -> str:
        return self._dados.nome

    @property
    def estado(self) -> EstadoJob:
        return self._estado

    @estado.setter
    def estado(self, e: EstadoJob):
        self._estado = e
