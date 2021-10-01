import logging
import pytest
import pathlib
from os import chdir
from os.path import join
from dotenv import load_dotenv
from typing import List
from pytest_mock.plugin import MockerFixture

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.caso import Caso
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.modelos.caso import CasoDECOMP
from encadeador.controladores.monitorcaso import MonitorNEWAVE
from encadeador.controladores.monitorcaso import MonitorDECOMP

DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests", "_arquivos", "casos")
ARQ_CFG = "encadeia.cfg"
CAMINHO_TESTE = "/home/user"
ANO_TESTE = 2021
MES_TESTE = 1
REVISAO_TESTE = 0
log = logging.getLogger()


class GerenteChamadasTerminal:

    def __init__(self) -> None:
        self.num_chamadas = 0
        self.respostas = []

    def reseta_contagem_chamadas(self):
        self.num_chamadas = 0

    def mock_executa_terminal(self,
                              cmds: List[str],
                              timeout: float = 1):
        ret = []
        self.num_chamadas += 1
        if self.num_chamadas == len(self.respostas) + 1:
            ret = ["", "", ""]
            self.reseta_contagem_chamadas()
        else:
            ret = self.respostas[self.num_chamadas - 1]
        return 0, ret


def constroi_caso_configurado_teste(caso: Caso) -> Caso:
    load_dotenv(ARQ_CFG, override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    caso.configura_caso(CAMINHO_TESTE,
                        ANO_TESTE,
                        MES_TESTE,
                        REVISAO_TESTE,
                        cfg)
    return caso


def test_monitor_newave_nao_inicializado():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        m = MonitorNEWAVE(c, log)


def test_monitor_newave_inicializado():
    chdir(DIR_TESTE)
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    m = MonitorNEWAVE(c, log)
    chdir(DIR_INICIAL)


def test_monitor_decomp_nao_inicializado():
    with pytest.raises(ValueError):
        c = CasoDECOMP()
        m = MonitorDECOMP(c, log)


def test_monitor_decomp_inicializado():
    chdir(DIR_TESTE)
    c = constroi_caso_configurado_teste(CasoDECOMP())
    m = MonitorDECOMP(c, log)
    chdir(DIR_INICIAL)


def test_monitor_newave_execucao_sucesso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    log = logging.getLogger()
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    m = MonitorNEWAVE(c, log)
    log = logging.getLogger()
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("pmo") has been submitted'],
                   ["", "",
                   "123 0.00000 pmo        pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "]
                  ]
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect = g.mock_executa_terminal)
    mocker.patch("encadeador.controladores.monitorcaso.ArmazenadorCaso" +
                 ".armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    MonitorNEWAVE.INTERVALO_POLL = 1
    r = m.executa_caso()
    chdir(DIR_INICIAL)
    assert m.caso.numero_tentativas == 1
    assert r


def test_monitor_newave_execucao_timeout_comunicacao(mocker: MockerFixture):
    chdir(DIR_TESTE)
    log = logging.getLogger()
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    m = MonitorNEWAVE(c, log)
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("pmo") has been submitted'],
                   ["", "",
                   "123 0.00000 pmo        pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "],
                   ['Job 123 ("pmo") registered for deletion'],
                   ["", "", ""]
                  ]
    g.respostas = 3 * g.respostas
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect=g.mock_executa_terminal)
    mocker.patch("encadeador.controladores.gerenciadorfila.getmtime",
                 return_value=0.)
    mocker.patch("encadeador.controladores.gerenciadorfila.isfile",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso.ArmazenadorCaso" +
                 ".armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    MonitorNEWAVE.INTERVALO_POLL = 1
    r = m.executa_caso()
    chdir(DIR_INICIAL)
    assert m.caso.numero_tentativas == 3
    assert not r


def test_monitor_decomp_execucao_sucesso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    log = logging.getLogger()
    c = constroi_caso_configurado_teste(CasoDECOMP())
    m = MonitorDECOMP(c, log)
    log = logging.getLogger()
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("pmo") has been submitted'],
                   ["", "",
                   "123 0.00000 pmo        pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "]
                  ]
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect=g.mock_executa_terminal)
    mocker.patch("encadeador.controladores.monitorcaso.ArmazenadorCaso" +
                 ".armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=True)
    MonitorDECOMP.INTERVALO_POLL = 1
    r = m.executa_caso()
    chdir(DIR_INICIAL)
    assert m.caso.numero_tentativas == 1
    assert r


def test_monitor_decomp_execucao_timeout_comunicacao(mocker: MockerFixture):
    chdir(DIR_TESTE)
    log = logging.getLogger()
    c = constroi_caso_configurado_teste(CasoDECOMP())
    m = MonitorDECOMP(c, log)
    log = logging.getLogger()
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("pmo") has been submitted'],
                   ["", "",
                   "123 0.00000 pmo        pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 pmo        pem          r     09/22/2021" +
                   " 13:17:19                                   72           "],
                   ['Job 123 ("pmo") registered for deletion'],
                   ["", "", ""]
                  ]
    g.respostas = 3 * g.respostas
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect=g.mock_executa_terminal)
    mocker.patch("encadeador.controladores.gerenciadorfila.getmtime",
                 return_value=0.)
    mocker.patch("encadeador.controladores.gerenciadorfila.isfile",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso.ArmazenadorCaso" +
                 ".armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=True)
    MonitorDECOMP.INTERVALO_POLL = 1
    r = m.executa_caso()
    chdir(DIR_INICIAL)
    assert m.caso.numero_tentativas == 3
    assert not r
