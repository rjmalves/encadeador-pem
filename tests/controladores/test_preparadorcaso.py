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
from encadeador.controladores.preparadorcaso import PreparadorCasoNEWAVE
from encadeador.controladores.preparadorcaso import PreparadorCasoDECOMP


DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests/_arquivos/casos")

CAMINHO_TESTE_NW = join(DIR_TESTE, "2021_01_rv0/newave")
CAMINHO_TESTE_DCP = join(DIR_TESTE, "2021_01_rv0/decomp")
log = logging.getLogger()


def test_preparador_caso_nao_inicializado():
    chdir(DIR_TESTE)
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        p = PreparadorCasoNEWAVE(c, log)
        p.prepara_caso()
    chdir(DIR_INICIAL)


def test_preparador_newave():
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    p = PreparadorCasoNEWAVE(c, log)
    r = p.prepara_caso()
    assert r
    chdir(DIR_INICIAL)


def test_preparador_decomp(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    c_ant = ArmazenadorCaso.recupera_caso(cfg,
                                          CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    p = PreparadorCasoDECOMP(c, log)
    m = mocker.patch("encadeador.controladores.preparadorcaso" +
                     ".SintetizadorCasoNEWAVE.verifica_cortes_extraidos")
    m.return_value = True
    r = p.prepara_caso(caso_cortes=c_ant)
    assert r
    chdir(DIR_INICIAL)
