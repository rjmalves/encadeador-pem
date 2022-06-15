# Casos de uso com Job (a julgar o que é
# responsabilidade do modelo):

# - Criado novo job para executar um caso
# - Submetido à fila
# - Atualização de estado
# - Possivel timeout
# - Deleção
# - Saída da fila por sucesso ou por deleção
# - Erro em qualquer etapa

from datetime import datetime, timedelta
from encadeador.modelos.job2 import Job
from encadeador.modelos.estadojob import EstadoJob


def cria_job(e: EstadoJob, t_entrada=None, t_inicio=None, t_saida=None) -> Job:
    t_inicio = datetime.now() if t_inicio is None else t_inicio
    t_entrada = (
        t_inicio - timedelta(seconds=30) if t_entrada is None else t_entrada
    )
    t_saida = t_inicio + timedelta(seconds=30) if t_saida is None else t_saida
    return Job(
        1,
        "Teste",
        "/home/teste",
        t_entrada,
        t_inicio,
        t_saida,
        10,
        e,
        0,
    )


def test_job_entrada_fila_atualiza_tempos():
    t = datetime.now()
    j = cria_job(EstadoJob.NAO_INICIADO, t - timedelta(days=7))
    j.atualiza(EstadoJob.ESPERANDO, t=t)
    assert j._instante_entrada_fila == t


def test_job_inicio_execucao_atualiza_tempos():
    t = datetime.now()
    j = cria_job(EstadoJob.ESPERANDO, None, t - timedelta(days=7))
    j.atualiza(EstadoJob.EXECUTANDO, t=t)
    assert j._instante_inicio_execucao == t


def test_job_fim_execucao_atualiza_tempos():
    t = datetime.now()
    j = cria_job(EstadoJob.EXECUTANDO, None, None, t + timedelta(days=7))
    j.atualiza(EstadoJob.FINALIZADO, t=t)
    assert j._instante_saida_fila == t


def test_job_nao_iniciado_tempos_nulos():
    j = cria_job(EstadoJob.NAO_INICIADO)
    assert j.tempo_fila == timedelta(0)
    assert j.tempo_execucao == timedelta(0)


def test_job_esperando_tempos_corretos():
    j = cria_job(EstadoJob.ESPERANDO)
    assert j.tempo_fila > timedelta(0)
    assert j.tempo_execucao == timedelta(0)


def test_job_executando_tempos_corretos():
    j = cria_job(EstadoJob.EXECUTANDO)
    assert j.tempo_fila == timedelta(seconds=30)
    assert j.tempo_execucao > timedelta(0)


def test_job_deletando_tempos_corretos():
    j = cria_job(EstadoJob.DELETANDO)
    assert j.tempo_fila == timedelta(seconds=30)
    assert j.tempo_execucao > timedelta(0)


def test_job_erro_tempos_corretos():
    j = cria_job(EstadoJob.ERRO)
    assert j.tempo_fila == timedelta(seconds=30)
    assert j.tempo_execucao == timedelta(seconds=30)


def test_job_timeout_tempos_corretos():
    j = cria_job(EstadoJob.TIMEOUT)
    assert j.tempo_fila == timedelta(seconds=30)
    assert j.tempo_execucao > timedelta(0)


def test_job_finalizado_tempos_corretos():
    j = cria_job(EstadoJob.FINALIZADO)
    assert j.tempo_fila == timedelta(seconds=30)
    assert j.tempo_execucao == timedelta(seconds=30)


def test_job_ativo():
    j = cria_job(EstadoJob.NAO_INICIADO)
    for e in [EstadoJob.NAO_INICIADO, EstadoJob.FINALIZADO]:
        j._estado = e
        assert not j.ativo
    for e in [
        EstadoJob.ESPERANDO,
        EstadoJob.EXECUTANDO,
        EstadoJob.ERRO,
        EstadoJob.TIMEOUT,
        EstadoJob.DELETANDO,
    ]:
        j._estado = e
        assert j.ativo
