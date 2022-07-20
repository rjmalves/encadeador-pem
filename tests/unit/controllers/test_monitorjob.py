from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
from typing import List

from encadeador.modelos.job2 import Job
from encadeador.modelos.estadojob import EstadoJob
from encadeador.services.unitofwork.job import JSONJobUnitOfWork
from encadeador.controladores.monitorjob2 import MonitorJob



def test_inicializa_monitor_job():
    g = MonitorJob(JSONJobUnitOfWork(""))
    m = MagicMock()
    g.observa(m)
    g.inicializa()
    m.assert_called_once()

# @patch("")
# def test_submete_monitor_job():
#     g = MonitorJob(JSONJobUnitOfWork(""))
#     m = MagicMock()
#     g.observa(m)
#     g.inicializa()
#     m.assert_called_once()

