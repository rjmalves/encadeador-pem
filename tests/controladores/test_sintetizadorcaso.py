import logging
import pytest
import pathlib
from dotenv import load_dotenv
from os import chdir
from os.path import join
from pytest_mock.plugin import MockerFixture

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorCasoNEWAVE
from encadeador.controladores.sintetizadorcaso import SintetizadorCasoDECOMP

DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests", "_arquivos", "casos")

CAMINHO_TESTE_NW = join(DIR_TESTE, "2021_01_rv0", "newave")
CAMINHO_TESTE_DCP = join(DIR_TESTE, "2021_01_rv0", "decomp")
ANO_TESTE = 2021
MES_TESTE = 1
REVISAO_TESTE = 0
log = logging.getLogger()


def test_sintetizador_caso_nao_inicializado():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        s = SintetizadorCasoNEWAVE(c, log)


def test_sintetizador_newave():
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    s = SintetizadorCasoNEWAVE(c, log)
    r = s.sintetiza_caso()
    chdir(DIR_INICIAL)
    assert r


def test_sintetizador_decomp():
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    s = SintetizadorCasoDECOMP(c, log)
    r = s.sintetiza_caso()
    chdir(DIR_INICIAL)
    assert r


def test_sintetizador_extrai_deleta_cortes_sucesso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    s = SintetizadorCasoNEWAVE(c, log)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorCasoNEWAVE._nomes_arquivos_cortes",
                 return_value=["cortes.dat", "cortesh.dat"])
    if not s.verifica_cortes_extraidos():
        s.extrai_cortes()
    if s.verifica_cortes_extraidos():
        s.deleta_cortes()
    chdir(DIR_INICIAL)
