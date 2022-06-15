from abc import abstractmethod
from os.path import getmtime, isfile
from typing import Callable, List, Optional
from datetime import datetime, timedelta

from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.job2 import Job
from encadeador.utils.log import Log
from encadeador.utils.terminal import executa_terminal_retry
from encadeador.utils.event import Event


class GerenciadorFila:
    """
    Interface base para representação dos gerenciadores de fila
    para execução dos casos no estudo encadeado.
    """

    # TODO - O gerenciador de fila hoje tem mais de uma responsabilidade.
    # 1) Traduz os comandos desejados do job para o ambiente
    # em que ele é executado (submetes, conferir estado, etc.) - OK
    # 2) Guarda a configuração de tempo de timeout - N OK
    # 3) Publica o evento de transicao de caso - N OK
    # 4) ...

    TIMEOUT_COMUNICACAO = timedelta(minutes=30)

    def __init__(self):
        self.__job: Optional[Job] = None
        self._arquivo_stdout: Optional[str] = None
        self._arquivo_stderr: Optional[str] = None
        self._mudou_estado = Event()

    # Factory Method
    @staticmethod
    def factory(ger: str) -> "GerenciadorFila":
        if ger == "SGE":
            return GerenciadorFilaSGE()
        else:
            raise ValueError(f"Gerenciador de fila '{ger}' não suportado")

    def observa(self, f: Callable):
        self._mudou_estado.append(f)

    @property
    def id_job(self) -> int:
        return self.__job.codigo

    @property
    def nome_job(self) -> str:
        return self.__job.nome

    @property
    def arquivo_stdout(self) -> str:
        """
        Nome do arquivo de saída padrão que o gerenciador de
        filas utiliza para escrita.

        :return: Nome do arquivo.
        :rtype: str
        """
        return self._arquivo_stdout

    @property
    def arquivo_stderr(self) -> str:
        """
        Nome do arquivo de saída de erro que o gerenciador de
        filas utiliza para escrita.

        :return: Nome do arquivo.
        :rtype: str
        """
        return self._arquivo_stderr

    @property
    def tempo_job_idle(self) -> timedelta:
        """
        A diferença entre o instante de tempo atual e o instante
        da última escrita feita pelo job no arquivo de saída padrão.

        :return: Tempo desde a última escrita de saída do job.
        :rtype: timedelta
        """
        if not isfile(self.arquivo_stdout):
            return 0.0
        else:
            return datetime.now() - datetime.fromtimestamp(
                getmtime(self.arquivo_stdout)
            )

    @abstractmethod
    def comando_qsub(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def comando_qstat(self) -> List[str]:
        raise NotImplementedError

    @abstractmethod
    def comando_qdel(self) -> List[str]:
        raise NotImplementedError

    def inicia(
        self, caminho_job: str, nome_job: str, num_processadores: int
    ) -> bool:
        """
        Solicita a inclusão de um job no gerenciamento
        de filas.

        :return: Sucesso ou não da inclusão.
        :rtype: bool
        """
        self._comandos = self.comando_qsub(
            caminho_job, nome_job, num_processadores
        )
        cod, self._respostas = executa_terminal_retry(self._comandos)
        if cod != 0:
            self._inicializa_gerenciador()
        return cod == 0

    def deleta_job(self) -> bool:
        cod, _ = executa_terminal_retry(self.comando_qdel())
        return cod == 0

    @abstractmethod
    def _inicializa_gerenciador(self):
        """
        Interpreta as saídas do comando de agendamento do job
        e inicializa os atributos do gerenciador de fila.
        """
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
    def _extrai_estado_job(self):
        raise NotImplementedError

    def monitora_estado_job(self):
        Log.log().info("Monitorando - fila...")
        estado = self._extrai_estado_job()
        if self._estado_timeout(estado):
            self.estado_job = EstadoJob.TIMEOUT
        elif self._estado_erro(estado):
            self.estado_job = EstadoJob.ERRO
        elif self._estado_finalizado(estado):
            self.estado_job = EstadoJob.FINALIZADO
        elif self._estado_esperando(estado):
            self.estado_job = EstadoJob.ESPERANDO
        elif self._estado_executando(estado):
            self.estado_job = EstadoJob.EXECUTANDO
        elif self._estado_deletando(estado):
            self.estado_job = EstadoJob.DELETANDO
        else:
            raise ValueError(f"Estado não reconhecido: {estado}")

    @property
    def estado_job(self) -> EstadoJob:
        """
        Retorna uma interpretação do estado do job
        no gerenciador de filas.

        :return: Estado do job no gerenciador de filas.
        :rtype: EstadoJob
        """
        return self.__job.estado

    @estado_job.setter
    def estado_job(self, e: EstadoJob):
        if self.__job.estado == e:
            return
        self.__job.estado = e
        self._mudou_estado(e)


class GerenciadorFilaSGE(GerenciadorFila):
    """
    Interface com o gerenciador de fila Sun Grid Engine
    para gerenciar casos.
    """

    def __init__(self):
        super().__init__()

    def _inicializa_gerenciador(self):
        if self._respostas is None:
            raise ValueError("Gerenciador de filas não inicializado!")
        resposta = self._respostas[0]
        # id_job
        self._id_job = int(resposta.split("Your job")[1].split("(")[0].strip())
        # nome_job
        self._nome_job = resposta.split("(")[1].split(")")[0].strip('"')
        # arquivos stdout e stderr
        self._arquivo_stdout = f"{self.nome_job}.o{self.id_job}"
        self._arquivo_stderr = f"{self.nome_job}.e{self.id_job}"

    def comando_qstat(self) -> List[str]:
        return ["qstat"]

    def comando_qsub(
        self, caminho_job: str, nome_job: str, num_processadores: int
    ) -> List[str]:
        return [
            "qsub",
            "-cwd",
            "-V",
            "-N",
            nome_job,
            "-pe",
            "orte",
            str(num_processadores),
            caminho_job,
            str(num_processadores),
        ]

    def comando_qdel(self) -> List[str]:
        return ["qdel", str(self.id_job)]

    # Override
    def _extrai_estado_job(self):
        cod, saidas = executa_terminal_retry(self.comando_qstat())
        if cod != 0:
            raise ValueError(f"Erro na execução do qstat: código {cod}")
        estado = ""
        for linha in saidas[2:]:
            lin = linha.strip()
            if lin.split(" ")[0] == "":
                break
            if int(lin.split(" ")[0]) == self.id_job:
                estado = linha[34:38].strip()
                break
        return estado

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
        return self.estado_job != EstadoJob.NAO_INICIADO and e == ""
