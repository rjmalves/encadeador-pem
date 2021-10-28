import pathlib
from os import chdir
from os.path import join
from logging import getLogger
from dotenv import load_dotenv
from unittest.mock import PropertyMock
from pytest_mock.plugin import MockerFixture

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.app import App

DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests", "_arquivos", "casos")
CAMINHO_TESTE_NW = join(DIR_TESTE, "2021_01_rv0", "newave")
CAMINHO_TESTE_DCP = join(DIR_TESTE, "2021_01_rv0", "decomp")
ARQ_CFG = join(DIR_TESTE, "encadeia_app.cfg")


def test_app_erro_construcao_arvore(mocker: MockerFixture):
    log = getLogger()
    chdir(DIR_TESTE)
    load_dotenv(ARQ_CFG, override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    mocker.patch("encadeador.app.ArvoreCasos.le_arquivo_casos",
                 return_value=None)
    mocker.patch("encadeador.app.ArvoreCasos.constroi_casos",
                 return_value=False)
    a = App(cfg, log)
    r = a.executa()
    assert not r
    chdir(DIR_INICIAL)


def test_app_executa_caso_erro(mocker: MockerFixture):
    log = getLogger()
    chdir(DIR_TESTE)
    load_dotenv(ARQ_CFG, override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.app.ArvoreCasos.le_arquivo_casos",
                 return_value=None)
    mocker.patch("encadeador.app.ArvoreCasos.proximo_caso",
                 new_callable=PropertyMock,
                 return_value=c)
    mocker.patch("encadeador.app.ArvoreCasos.ultimo_newave",
                 new_callable=PropertyMock,
                 return_value=None)
    mocker.patch("encadeador.app.ArvoreCasos.ultimo_decomp",
                 new_callable=PropertyMock,
                 return_value=None)
    mocker.patch("encadeador.app.ArvoreCasos.terminou",
                 new_callable=PropertyMock,
                 return_value=False)
    mocker.patch("encadeador.app.ArvoreCasos.constroi_casos",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ExecutorNEWAVE.executa_e_monitora_caso",
                 side_effect=RuntimeError())
    a = App(cfg, log)
    r = a.executa()
    assert not r
    chdir(DIR_INICIAL)


def test_app_executa_caso_sucesso(mocker: MockerFixture):
    log = getLogger()
    chdir(DIR_TESTE)
    load_dotenv(ARQ_CFG, override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.app.ArvoreCasos.le_arquivo_casos",
                 return_value=None)
    mocker.patch("encadeador.app.ArvoreCasos.proximo_caso",
                 new_callable=PropertyMock,
                 return_value=c)
    mocker.patch("encadeador.app.ArvoreCasos.ultimo_newave",
                 new_callable=PropertyMock,
                 return_value=None)
    mocker.patch("encadeador.app.ArvoreCasos.ultimo_decomp",
                 new_callable=PropertyMock,
                 return_value=None)
    mocker.patch("encadeador.app.ArvoreCasos.terminou",
                 new_callable=PropertyMock,
                 return_value=True)
    mocker.patch("encadeador.app.ArvoreCasos.constroi_casos",
                 return_value=True)
    a = App(cfg, log)
    r = a.executa()
    assert r
    chdir(DIR_INICIAL)
