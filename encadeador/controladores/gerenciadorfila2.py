from abc import abstractmethod
from os.path import getmtime, isfile
from typing import List
from datetime import datetime, timedelta

from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.job2 import Job
from encadeador.utils.log import Log
from encadeador.utils.terminal import executa_terminal_retry


class GerenciadorFila:
    """
    Interface base para representação dos gerenciadores de fila
    para execução dos casos no estudo encadeado.
    """

    TIMEOUT_COMUNICACAO = timedelta(minutes=30)

    def __init__(self, job: Job):
        self._job = job

    # Factory Method
    @staticmethod
    def factory(ger: str, job: Job) -> "GerenciadorFila":
        if ger == "SGE":
            return GerenciadorFilaSGE(job)
        else:
            raise ValueError(f"Gerenciador de fila '{ger}' não suportado")

    @property
    @abstractmethod
    def arquivo_stdout(self) -> str:
        """
        Nome do arquivo de saída padrão que o gerenciador de
        filas utiliza para escrita.
        """
        raise NotImplementedError

    @staticmethod
    def _ultima_modificacao_arquivo(arquivo: str) -> datetime:
        if not isfile(arquivo):
            return datetime.now()
        else:
            return datetime.fromtimestamp(getmtime(arquivo))

    @property
    def tempo_job_idle(self) -> timedelta:
        """
        A diferença entre o instante de tempo atual e o instante
        da última escrita feita pelo job no arquivo de saída padrão.
        """
        return datetime.now() - GerenciadorFila._ultima_modificacao_arquivo(
            self.arquivo_stdout
        )

    @property
    @abstractmethod
    def _comando_qsub(self) -> List[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def _comando_qstat(self) -> List[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def _comando_qdel(self) -> List[str]:
        raise NotImplementedError

    def inicia(self) -> bool:
        """
        Solicita a inclusão de um job no gerenciamento
        de filas.

        :return: Sucesso ou não da inclusão.
        :rtype: bool
        """
        comandos = self._comando_qsub
        cod, respostas = executa_terminal_retry(comandos)
        sucesso = cod == 0
        if sucesso:
            self._processa_sucesso_submissao(respostas)
        return sucesso

    def deleta(self) -> bool:
        cod, _ = executa_terminal_retry(self._comando_qdel)
        return cod == 0

    @abstractmethod
    def _processa_sucesso_submissao(self, respostas: List[str]):
        raise NotImplementedError

    @abstractmethod
    def _estado_timeout(self, e: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _estado_esperando(self, e: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _estado_executando(self, e: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _estado_deletando(self, e: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _estado_erro(self, e: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _estado_finalizado(self, e: str) -> bool:
        raise NotImplementedError

    @abstractmethod
    def _extrai_caracteres_estado(self):
        raise NotImplementedError

    def monitora(self) -> EstadoJob:
        e = EstadoJob.NAO_INICIADO
        c = self._extrai_caracteres_estado()
        if self._estado_timeout(c):
            e = EstadoJob.TIMEOUT
        elif self._estado_erro(c):
            e = EstadoJob.ERRO
        elif self._estado_finalizado(c):
            e = EstadoJob.FINALIZADO
        elif self._estado_esperando(c):
            e = EstadoJob.ESPERANDO
        elif self._estado_executando(c):
            e = EstadoJob.EXECUTANDO
        elif self._estado_deletando(c):
            e = EstadoJob.DELETANDO
        return e


class GerenciadorFilaSGE(GerenciadorFila):
    """
    Interface com o gerenciador de fila Sun Grid Engine
    para gerenciar casos.
    """

    def __init__(self, job: Job):
        super().__init__(job)

    @property
    def arquivo_stdout(self) -> str:
        """
        Nome do arquivo de saída padrão que o gerenciador de
        filas utiliza para escrita.
        """
        return f"{self._job.nome}.o{self._job.codigo}"

    @property
    def _comando_qstat(self) -> List[str]:
        return ["qstat"]

    @property
    def _comando_qsub(self) -> List[str]:
        return [
            "qsub",
            "-cwd",
            "-V",
            "-N",
            self._job.nome,
            "-pe",
            "orte",
            str(self._job.numero_processadores),
            self._job.caminho,
            str(self._job.numero_processadores),
        ]

    @property
    def _comando_qdel(self) -> List[str]:
        return ["qdel", str(self._job.id)]

    # Override
    def _extrai_caracteres_estado(self) -> str:
        cod, saidas = executa_terminal_retry(self._comando_qstat)
        if cod != 0:
            raise ValueError(f"Erro na execução do qstat: código {cod}")
        estado = ""
        for linha in saidas[2:]:
            lin = linha.strip()
            if lin.split(" ")[0] == "":
                break
            if int(lin.split(" ")[0]) == self._job.codigo:
                dados = [d for d in linha.split(" ") if len(d) > 0]
                if len(dados[4]) < 4:
                    estado = dados[4].strip()
                break
        return estado

    def _processa_sucesso_submissao(self, respostas: List[str]):
        resposta = respostas[0]
        self._job.codigo = int(
            resposta.split("Your job")[1].split("(")[0].strip()
        )
        self._job.nome = resposta.split("(")[1].split(")")[0].strip('"')

    def _estado_timeout(self, e: str) -> bool:
        return (
            e == "r"
            and self.tempo_job_idle >= self.__class__.TIMEOUT_COMUNICACAO
        )

    def _estado_esperando(self, e: str) -> bool:
        return e in ["qw", "t", "q"]

    def _estado_executando(self, e: str) -> bool:
        return (
            e == "r"
            and self.tempo_job_idle < self.__class__.TIMEOUT_COMUNICACAO
        )

    def _estado_deletando(self, e: str) -> bool:
        return e in ["dr", "d"]

    def _estado_erro(self, e: str) -> bool:
        return "e" in e

    def _estado_finalizado(self, e: str) -> bool:
        return self._job.estado != EstadoJob.NAO_INICIADO and e == ""
