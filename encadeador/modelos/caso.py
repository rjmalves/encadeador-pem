from abc import abstractmethod
from typing import Dict, List, Any

from encadeador.modelos.dadoscaso import DadosCaso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.job import Job
from encadeador.modelos.estadocaso import EstadoCaso


class Caso:
    """
    Classe base que representa todos os casos executados no
    estudo encadeado. É responsável por armazenar informações
    associadas ao caso. Não lida com a sua execução, pré ou pós
    processamento dos decks.
    """

    def __init__(
        self,
        dados: DadosCaso,
        jobs: List[Job],
        estado: EstadoCaso = EstadoCaso.NAO_INICIADO,
    ) -> None:
        self._dados = dados
        self._jobs = jobs
        self._estado = estado

    @staticmethod
    def factory(
        dados: DadosCaso,
        jobs: List[Job],
        estado: EstadoCaso = EstadoCaso.NAO_INICIADO,
    ) -> "Caso":
        if dados.programa == "NEWAVE":
            return CasoNEWAVE(dados, jobs, estado)
        elif dados.programa == "DECOMP":
            return CasoDECOMP(dados, jobs, estado)
        else:
            raise ValueError(f"Programa {dados.programa} não suportado")

    @staticmethod
    def from_json(json_dict: Dict[str, Any]):
        dados = DadosCaso.from_json(json_dict["_dados"])
        jobs = [Job.from_json(j) for j in json_dict["_jobs"]]
        estado = EstadoCaso.factory(json_dict["_estado"])
        return Caso.factory(dados, jobs, estado)

    def to_json(self) -> Dict[str, Any]:
        return {
            "_dados": self._dados.to_json(),
            "_jobs": [j.to_json() for j in self._jobs],
            "_estado": str(self._estado.value),
        }

    def atualiza(self, estado: EstadoCaso):
        self._estado = estado

    def adiciona_job(self, job: Job, retry: bool):
        if retry:
            self._jobs[-1] = job
        else:
            self._jobs.append(job)

    @staticmethod
    @abstractmethod
    def gera_dados_caso(caminho: str, ano: int, mes: int, revisao: int):
        pass

    @abstractmethod
    def _constroi_nome_caso(self, ano: int, mes: int, revisao: int) -> str:
        pass

    def _verifica_caso_configurado(self):
        if self._dados is None:
            raise ValueError("Caso não configurado!")

    @property
    def caminho(self) -> str:
        self._verifica_caso_configurado()
        return self._dados.caminho

    @caminho.setter
    def caminho(self, c: str):
        self._dados.caminho = c

    @property
    def nome(self) -> str:
        self._verifica_caso_configurado()
        return self._dados.nome

    @property
    def tempo_fila(self) -> float:
        self._verifica_caso_configurado()
        return sum([j.tempo_fila for j in self._jobs])

    @property
    def tempo_execucao(self) -> float:
        self._verifica_caso_configurado()
        return sum([j.tempo_execucao for j in self._jobs])

    @property
    def ano(self) -> int:
        self._verifica_caso_configurado()
        return self._dados.ano

    @property
    def mes(self) -> int:
        self._verifica_caso_configurado()
        return self._dados.mes

    @property
    def revisao(self) -> int:
        self._verifica_caso_configurado()
        return self._dados.revisao

    @property
    def numero_flexibilizacoes(self) -> int:
        return len(self._jobs) - 1

    @property
    def estado(self) -> EstadoCaso:
        return self._estado

    @property
    @abstractmethod
    def numero_processadores(self) -> int:
        pass


class CasoNEWAVE(Caso):
    @staticmethod
    def from_json(json_dict: Dict[str, Any]):
        return CasoNEWAVE(
            DadosCaso.from_json(json_dict["_dados"]),
            [Job.from_json(j) for j in json_dict["_jobs"]],
        )

    # Override
    @staticmethod
    def gera_dados_caso(caminho: str, ano: int, mes: int, revisao: int):
        nome = CasoNEWAVE._constroi_nome_caso(ano, mes, revisao)
        return DadosCaso("NEWAVE", caminho, nome, ano, mes, revisao)

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
    @staticmethod
    def from_json(json_dict: Dict[str, Any]):
        return CasoDECOMP(
            DadosCaso.from_json(json_dict["_dados"]),
            [Job.from_json(j) for j in json_dict["_jobs"]],
        )

    # Override
    @staticmethod
    def gera_dados_caso(caminho: str, ano: int, mes: int, revisao: int):
        nome = CasoDECOMP._constroi_nome_caso(ano, mes, revisao)
        return DadosCaso("DECOMP", caminho, nome, ano, mes, revisao)

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
