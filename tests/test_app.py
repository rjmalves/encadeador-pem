import pathlib
from typing import List
from os import chdir
from os.path import join
from logging import getLogger
from dotenv import load_dotenv
import pytest
from pytest_mock.plugin import MockerFixture

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.app import App

DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests/_arquivos/casos")
ARQ_CFG = join(DIR_TESTE, "encadeia_app.cfg")


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


def test_app_executa_caso_sumiu_fila(mocker: MockerFixture):
    log = getLogger()
    chdir(DIR_TESTE)
    load_dotenv(ARQ_CFG, override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("DC202111") has been submitted'],
                   ["", "",
                   "123 0.00000 DC202111   pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "123 0.00000 DC202111   pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "]
                  ]
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect = g.mock_executa_terminal)
    mocker.patch("encadeador.controladores.monitorcaso.ArmazenadorCaso" +
                 ".armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".SintetizadorCasoDECOMP.sintetiza_caso",
                 return_value=True)
    with pytest.raises(RuntimeError):
        a = App(cfg, log)
        a.executa()
    chdir(DIR_INICIAL)



def test_app_executa_caso_deletado(mocker: MockerFixture):
    log = getLogger()
    chdir(DIR_TESTE)
    load_dotenv(ARQ_CFG, override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("DC202111") has been submitted'],
                   ["", "",
                   "    123 0.00000 DC202111   pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "    123 0.00000 DC202111   pem          r     09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "    123 0.00000 DC202111   pem          dr    09/22/2021" +
                   " 13:17:19                                   72           "]
                  ]
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect = g.mock_executa_terminal)
    with pytest.raises(RuntimeError):
        a = App(cfg, log)
        a.executa()
    chdir(DIR_INICIAL)


def test_app_executa_max_flex_decomp(mocker: MockerFixture):
    log = getLogger()
    chdir(DIR_TESTE)
    load_dotenv(ARQ_CFG, override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    g = GerenteChamadasTerminal()
    g.respostas = [
                   ['Your job 123 ("DC202111") has been submitted'],
                   ["", "",
                   "    123 0.00000 DC202111   pem          qw    09/22/2021" +
                   " 13:17:19                                   72           "],
                   ["", "",
                   "    123 0.00000 DC202111   pem          r     09/22/2021" +
                   " 13:17:19                                   72           "]
                  ]
    mocker.patch("encadeador.controladores.gerenciadorfila.executa_terminal",
                 side_effect=g.mock_executa_terminal)
    with pytest.raises(RuntimeError):
        a = App(cfg, log)
        a.executa()
    chdir(DIR_INICIAL)
