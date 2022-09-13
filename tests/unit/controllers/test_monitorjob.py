from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from typing import List

from encadeador.modelos.job import Job
from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.transicaojob import TransicaoJob
from encadeador.services.unitofwork.job import JSONJobUnitOfWork
from encadeador.controladores.monitorjob import MonitorJob


def test_inicializa_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g.inicializa()
    m.assert_called_once()


@patch(
    "encadeador.services.handlers.job.submete",
    lambda c, uow: Job(
        1,
        "Teste",
        "/home/teste",
        datetime.now(),
        datetime.now(),
        datetime.now(),
        72,
        EstadoJob.EXECUTANDO,
        1,
    ),
)
def test_submete_sucesso_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g.submete("/home/teste", 72, 1, "TESTE")
    m.assert_called_once()


@patch(
    "encadeador.services.handlers.job.submete",
    lambda c, uow: Job(
        1,
        "Teste",
        "/home/teste",
        datetime.now(),
        datetime.now(),
        datetime.now(),
        72,
        EstadoJob.EXECUTANDO,
        1,
    ),
)
def test_submete_sucesso_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    assert g.submete("/home/teste", 72, 1, "TESTE")
    m.assert_called_once_with(TransicaoJob.SUBMISSAO_SOLICITADA)


@patch(
    "encadeador.services.handlers.job.submete",
    lambda c, uow: None,
)
def test_submete_erro_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    assert not g.submete("/home/teste", 72, 1, "TESTE")
    m.assert_called_with(TransicaoJob.SUBMISSAO_ERRO)


@patch(
    "encadeador.services.handlers.job.deleta",
    lambda c, uow: True,
)
def test_submete_sucesso_deleta_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    assert g.deleta("TESTE")


@patch(
    "encadeador.services.handlers.job.deleta",
    lambda c, uow: False,
)
def test_submete_erro_deleta_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    assert not g.deleta("TESTE")


@patch(
    "encadeador.services.handlers.job.monitora",
    lambda c, uow, cb: False,
)
def test_monitora_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    g.monitora("TESTE")


@patch("encadeador.utils.log.Log.log", lambda: MagicMock())
def test_handler_submissao_sucesso_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g._handler_submissao_sucesso()
    m.assert_called_once_with(TransicaoJob.SUBMISSAO_SUCESSO)


@patch("encadeador.utils.log.Log.log", lambda: MagicMock())
def test_handler_delecao_solicitada_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g._handler_delecao_solicitada()
    m.assert_called_once_with(TransicaoJob.DELECAO_SOLICITADA)


@patch("encadeador.utils.log.Log.log", lambda: MagicMock())
def test_handler_delecao_sucesso_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g._handler_delecao_sucesso()
    m.assert_called_once_with(TransicaoJob.DELECAO_SUCESSO)


@patch("encadeador.utils.log.Log.log", lambda: MagicMock())
def test_handler_inicio_execucao_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g._handler_inicio_execucao()
    m.assert_called_once_with(TransicaoJob.INICIO_EXECUCAO)


@patch("encadeador.utils.log.Log.log", lambda: MagicMock())
def test_handler_inicio_execucao_direto_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g._handler_inicio_execucao_direto()
    m.assert_called_with(TransicaoJob.INICIO_EXECUCAO)


@patch("encadeador.utils.log.Log.log", lambda: MagicMock())
def test_handler_fim_execucao_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g._handler_fim_execucao()
    m.assert_called_with(TransicaoJob.FIM_EXECUCAO)


@patch("encadeador.utils.log.Log.log", lambda: MagicMock())
def test_handler_delecao_erro_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g._handler_delecao_erro()
    m.assert_called_with(TransicaoJob.DELECAO_ERRO)


@patch("encadeador.utils.log.Log.log", lambda: MagicMock())
def test_handler_timeout_execucao_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g._handler_timeout_execucao()
    m.assert_called_with(TransicaoJob.TIMEOUT_EXECUCAO)
