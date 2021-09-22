import logging
import pytest
import time
from typing import List
from pytest_mock.plugin import MockerFixture

from encadeador.modelos.caso import Configuracoes
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.controladores.monitorcaso import MonitorNEWAVE

CAMINHO_TESTE = "/home/user"
NOME_TESTE = "teste"
ANO_TESTE = 2021
MES_TESTE = 1
REVISAO_TESTE = 0


def test_monitor_newave_nao_inicializado():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        m = MonitorNEWAVE(c)


def test_monitor_newave_inicializado():
    c = CasoNEWAVE()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     ANO_TESTE,
                     MES_TESTE,
                     REVISAO_TESTE,
                     Configuracoes())
    m = MonitorNEWAVE(c)


class GerenteChamadasTerminal:

    def __init__(self) -> None:
        self.NUM_CHAMADAS = 0
        self.respostas = []

    def reseta_contagem_chamadas(self):
        self.NUM_CHAMADAS = 0

    def mock_executa_terminal(self,
                              cmds: List[str],
                              timeout: float = 1):
        ret = []
        self.NUM_CHAMADAS += 1
        if self.NUM_CHAMADAS == len(self.respostas) + 1:
            ret = ["", "", ""]
            self.reseta_contagem_chamadas()
        else:
            ret = self.respostas[self.NUM_CHAMADAS - 1]
        return 0, ret


def test_monitor_newave_execucao_sucesso(mocker: MockerFixture):
    c = CasoNEWAVE()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     ANO_TESTE,
                     MES_TESTE,
                     REVISAO_TESTE,
                     Configuracoes())
    m = MonitorNEWAVE(c)
    log = logging.getLogger()
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("pmo") has been submitted'],
                   ["", "",
                   "    123 0.00000 pmo        pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "    123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "]
                  ]
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect = g.mock_executa_terminal)
    MonitorNEWAVE.INTERVALO_POLL = 1
    r = m.executa_caso(log)
    assert m.caso.numero_tentativas == 1
    assert r


def test_monitor_newave_execucao_timeout_comunicacao(mocker: MockerFixture):
    c = CasoNEWAVE()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     ANO_TESTE,
                     MES_TESTE,
                     REVISAO_TESTE,
                     Configuracoes())
    m = MonitorNEWAVE(c)
    log = logging.getLogger()
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("pmo") has been submitted'],
                   ["", "",
                   "    123 0.00000 pmo        pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "    123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "", ""]
                  ]
    g.respostas = 3 * g.respostas
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect=g.mock_executa_terminal)
    mocker.patch("encadeador.controladores.gerenciadorfila.getmtime",
                 return_value=0.)
    mocker.patch("encadeador.controladores.gerenciadorfila.isfile",
                 return_value=True)
    MonitorNEWAVE.INTERVALO_POLL = 2
    MonitorNEWAVE.MAX_RETRY = 2
    r = m.executa_caso(log)
    assert m.caso.numero_tentativas == 2
    assert not r
