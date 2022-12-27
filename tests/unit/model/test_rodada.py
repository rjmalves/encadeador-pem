from datetime import datetime, timedelta
from encadeador.modelos.rodada import Rodada
from encadeador.modelos.runstatus import RunStatus


def cria_rodada(e: RunStatus, t_inicio=None, t_fim=None) -> Rodada:
    t_inicio = datetime.now() if t_inicio is None else t_inicio
    t_fim = t_inicio + timedelta(seconds=30) if t_fim is None else t_fim
    return Rodada(
        "Teste",
        e,
        1,
        "/home/teste",
        t_inicio,
        t_fim,
        10,
        "NEWAVE",
        "v28",
        0,
    )


def test_rodada_ativa():
    j = cria_rodada(RunStatus.SUBMITTED)
    for e in [RunStatus.STARTING, RunStatus.RUNNING]:
        j.estado = e
        assert j.ativa
    for e in [
        RunStatus.SUCCESS,
        RunStatus.DATA_ERROR,
        RunStatus.RUNTIME_ERROR,
        RunStatus.COMMUNICATION_ERROR,
        RunStatus.INFEASIBLE,
        RunStatus.UNKNOWN,
    ]:
        j.estado = e
        assert not j.ativa
