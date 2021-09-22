from abc import abstractmethod
from os.path import getmtime, isfile
from typing import List
import time

from encadeador.modelos.estadojob import EstadoJob
from encadeador.utils.terminal import executa_terminal


class GerenciadorFila:
    """
    Interface base para representação dos gerenciadores de fila
    para execução dos casos no estudo encadeado.
    """
    TIMEOUT_DELETE = 120

    def __init__(self):
        self._id_job = None
        self._nome_job = None
        self._arquivo_stdout = None
        self._arquivo_stderr = None
        self._comandos = None
        self._respostas = None

    def __confere_inicializacao(self, valor):
        if valor is None:
            raise ValueError(f"Gerenciador de Fila não inicializado!")

    # Factory Method
    @staticmethod
    def factory(ger: str) -> 'GerenciadorFila':
        if ger == "SGE":
            return GerenciadorFilaSGE()
        else:
            raise ValueError(f"Gerenciador de fila '{ger}' não suportado")

    @property
    def id_job(self) -> int:
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

    def agenda_job(self,
                   caminho_job: str,
                   nome_job: str,
                   num_processadores: int
                   ) -> bool:
        """
        Solicita a inclusão de um job no gerenciamento
        de filas.

        :return: Sucesso ou não da inclusão.
        :rtype: bool
        """
        self._comandos = self.comando_qsub(caminho_job,
                                           nome_job,
                                           num_processadores)
        try:
            cod, self._respostas = executa_terminal(self._comandos)
        except TimeoutError:
            return False
        self._inicializa_gerenciador()
        return cod == 0

    @abstractmethod
    def _inicializa_gerenciador(self):
        """
        Interpreta as saídas do comando de agendamento do job
        e inicializa os atributos do gerenciador de fila.
        """
        pass

    @property
    @abstractmethod
    def estado_job(self) -> EstadoJob:
        """
        Retorna uma interpretação do estado do job
        no gerenciador de filas.

        :return: Estado do job no gerenciador de filas.
        :rtype: EstadoJob
        """
        pass

    @abstractmethod
    def deleta_job(self) -> bool:
        """
        Solicita a deleção do job no gerenciador de filas
        e aguarda pela realização.

        :return: Sucesso ou não da deleção do job.
        :rtype: bool
        """
        pass

    @abstractmethod
    def comando_qsub(self,
                     caminho_job: str,
                     nome_job: str,
                     num_processadores: int) -> List[str]:
        pass


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
        self._id_job = (int(resposta.split("Your job")[1]
                        .split("(")[0].strip()))
        # nome_job
        self._nome_job = (resposta.split('(')[1]
                          .split(')')[0].strip('"'))
        # arquivos stdout e stderr
        self._arquivo_stdout = f"{self.nome_job}.o{self.id_job}"
        self._arquivo_stderr = f"{self.nome_job}.e{self.id_job}"

    @property
    def estado_job(self) -> EstadoJob:

        def __procura_codigo_estado(saidas: List[str]):
            achou = False
            estado = ""
            for linha in saidas[2:]:
                lin = linha.strip()
                if lin.split(" ")[0] == "":
                    break
                if int(lin.split(" ")[0]) == self.id_job:
                    achou = True
                    estado = linha[35:45].strip()
                    break
            if not achou:
                raise KeyError(f"Não foi encontrado o job {self.id_job} na fila")
            return estado

        # Verificações de erro:
        try:
            cod, saidas = executa_terminal(["qstat"])
        except TimeoutError as e:
            raise e
        if cod != 0:
            raise ValueError(f"Erro na execução do qstat: código {cod}")
        # Sucesso:
        estado = __procura_codigo_estado(saidas)
        if "e" in estado:
            estadojob = EstadoJob.ERRO
        elif estado == "qw" or estado == "t":
            estadojob = EstadoJob.ESPERANDO
        elif estado == "r":
            estadojob = EstadoJob.EXECUTANDO
        elif estado == "dr":
            estadojob = EstadoJob.DELETANDO
        else:
            raise ValueError(f"Estado de job '{estado}' desconhecido!")
        return estadojob
        
    def deleta_job(self) -> bool:
        ti = time.time()
        try:
            while time.time() - ti < GerenciadorFilaSGE.TIMEOUT_DELETE:
                estado = self.estado_job
                if estado != EstadoJob.DELETANDO:
                    cod, _ = executa_terminal(["qdel", f"{self.id_job}"])
                time.sleep(5)
            return False
        except KeyError:
            return True

    def comando_qsub(self,
                     caminho_job: str,
                     nome_job: str,
                     num_processadores: int) -> List[str]:
        return ["qsub",
                "-cwd",
                "-V",
                "-N",
                nome_job,
                "-pe",
                "orte",
                str(num_processadores),
                caminho_job,
                str(num_processadores)
                ]
