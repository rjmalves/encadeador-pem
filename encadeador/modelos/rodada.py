from datetime import datetime, timedelta
from encadeador.modelos.runstatus import RunStatus
from encadeador.modelos.run import Run


class Rodada:
    """
    Classe que define uma rodada de um modelo, gerenciada
    por API e com diagnóstico automático.
    """

    def __init__(
        self,
        nome: str,
        estado: RunStatus,
        id_job: str,
        caminho: str,
        instante_inicio_execucao: datetime,
        instante_fim_execucao: datetime,
        numero_processadores: int,
        nome_programa: str,
        versao_programa: str,
        id_caso: int,
    ) -> None:
        self.id = None
        self.nome = nome
        self.estado = estado
        self.id_job = id_job
        self.caminho = caminho
        self.instante_inicio_execucao = instante_inicio_execucao
        self.instante_fim_execucao = instante_fim_execucao
        self.numero_processadores = numero_processadores
        self.nome_programa = nome_programa
        self.versao_programa = versao_programa
        self.id_caso = id_caso

    def __eq__(self, o: object):
        if not isinstance(o, Rodada):
            return False
        return all(
            [
                self.id == o.id,
                self.nome == o.nome,
                self.estado == o.estado,
                self.id_job == o.id_job,
                self.caminho == o.caminho,
                self.instante_inicio_execucao == o.instante_inicio_execucao,
                self.instante_fim_execucao == o.instante_fim_execucao,
                self.numero_processadores == o.numero_processadores,
                self.nome_programa == o.nome_programa,
                self.versao_programa == o.versao_programa,
                self.id_caso == o.id_caso,
            ]
        )

    def __gt__(self, o: object):
        if not isinstance(o, Rodada):
            raise TypeError
        return self.instante_inicio_execucao > o.instante_inicio_execucao

    @property
    def ativa(self) -> bool:
        return self.estado not in [
            RunStatus.SUCCESS,
            RunStatus.INFEASIBLE,
            RunStatus.DATA_ERROR,
            RunStatus.RUNTIME_ERROR,
            RunStatus.COMMUNICATION_ERROR,
            RunStatus.UNKNOWN,
        ]

    @property
    def tempo_execucao(self) -> float:
        tempo_fim = (
            self.instante_fim_execucao
            if self.instante_fim_execucao is not None
            else datetime.now()
        )
        delta = tempo_fim - self.instante_inicio_execucao
        return delta.total_seconds()

    @classmethod
    def from_run(cls, run: Run, id_caso: int) -> "Rodada":
        return cls(
            run.name,
            run.status,
            run.jobId,
            run.jobWorkingDirectory,
            run.jobStartTime,
            run.jobEndTime,
            run.jobReservedSlots,
            run.programName,
            run.programVersion,
            id_caso,
        )
