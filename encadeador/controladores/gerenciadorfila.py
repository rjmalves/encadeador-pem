from abc import abstractmethod
from os.path import getmtime, isfile
from typing import Callable, List
import time

from encadeador.modelos.estadojob import EstadoJob
from encadeador.utils.log import Log
from encadeador.utils.terminal import executa_terminal
from encadeador.utils.event import Event


class GerenciadorFila:
    """
    Interface base para representação dos gerenciadores de fila
    para execução dos casos no estudo encadeado.
    """

    TIMEOUT_COMUNICACAO = 1800
    TIMEOUT_DELETE = 120

    def __init__(self):
        self._id_job = None
        self._nome_job = None
        self._arquivo_stdout = None
        self._arquivo_stderr = None
        self._comandos = None
        self._respostas = None
        self._estado_job = EstadoJob.NAO_INICIADO
        self._mudou_estado = Event()

    def __confere_inicializacao(self, valor):
        if valor is None:
            raise ValueError("Gerenciador de Fila não inicializado!")

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
    def id_job(self) -> str:
        self.__confere_inicializacao(self._id_job)
        return self._id_job

    @property
    def nome_job(self) -> str:
        self.__confere_inicializacao(self._nome_job)
        return self._nome_job

    @property
    def arquivo_stdout(self) -> str:
        """
        Nome do arquivo de saída padrão que o gerenciador de
        filas utiliza para escrita.

        :return: Nome do arquivo.
        :rtype: str
        """
        self.__confere_inicializacao(self._arquivo_stdout)
        return self._arquivo_stdout

    @property
    def arquivo_stderr(self) -> str:
        """
        Nome do arquivo de saída de erro que o gerenciador de
        filas utiliza para escrita.

        :return: Nome do arquivo.
        :rtype: str
        """
        self.__confere_inicializacao(self._arquivo_stderr)
        return self._arquivo_stderr

    @property
    def tempo_job_idle(self) -> float:
        """
        A diferença entre o instante de tempo atual e o instante
        da última escrita feita pelo job no arquivo de saída padrão.

        :return: Tempo em segundos que o job não realiza nenhuma
            escrita de saída.
        :rtype: float
        """
        if not isfile(self.arquivo_stdout):
            return 0.0
        else:
            return time.time() - getmtime(self.arquivo_stdout)

    @abstractmethod
    def comando_qsub(
        self, caminho_job: str, nome_job: str, num_processadores: int
    ) -> List[str]:
        pass

    @abstractmethod
    def comando_qstat(self) -> List[str]:
        pass

    @abstractmethod
    def comando_qdel(self) -> List[str]:
        pass

    def agenda_job(
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
        try:
            cod, self._respostas = executa_terminal(self._comandos)
        except TimeoutError:
            return False
        self._inicializa_gerenciador()
        return cod == 0

    def deleta_job(self) -> bool:
        cod, _ = executa_terminal(self.comando_qdel())
        return cod == 0

    @abstractmethod
    def _inicializa_gerenciador(self):
        """
        Interpreta as saídas do comando de agendamento do job
        e inicializa os atributos do gerenciador de fila.
        """
        pass

    @abstractmethod
    def _estado_timeout(self, e: str) -> bool:
        pass

    @abstractmethod
    def _estado_esperando(self, e: str) -> bool:
        pass

    @abstractmethod
    def _estado_executando(self, e: str) -> bool:
        pass

    @abstractmethod
    def _estado_deletando(self, e: str) -> bool:
        pass

    @abstractmethod
    def _estado_erro(self, e: str) -> bool:
        pass

    @abstractmethod
    def _estado_finalizado(self, e: str) -> bool:
        pass

    @abstractmethod
    def _extrai_estado_job(self):
        pass

    def monitora_estado_job(self):

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
        return self._estado_job

    @estado_job.setter
    def estado_job(self, e: EstadoJob):
        if self._estado_job == e:
            return
        self._estado_job = e
        Log.log().info(
            f"GerenciadorFila: Job {self.id_job} - Novo estado: {e.value}"
        )
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
        try:
            cod, saidas = executa_terminal(self.comando_qstat())
        except TimeoutError as e:
            raise e
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
