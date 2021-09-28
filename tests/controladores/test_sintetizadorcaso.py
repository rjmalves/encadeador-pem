import logging
import pytest

from encadeador.modelos.caso import Configuracoes
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorCasoNEWAVE
from encadeador.controladores.sintetizadorcaso import SintetizadorCasoDECOMP

CAMINHO_TESTE_NW = "./tests/_arquivos/casos/2021_01_rv0/newave"
CAMINHO_TESTE_DCP = "./tests/_arquivos/casos/2021_01_rv0/decomp"
ANO_TESTE = 2021
MES_TESTE = 1
REVISAO_TESTE = 0
log = logging.getLogger()


def test_sintetizador_caso_nao_inicializado():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        s = SintetizadorCasoNEWAVE(c, log)


def test_sintetizador_newave():
    c = ArmazenadorCaso.recupera_caso(Configuracoes(),
                                      CAMINHO_TESTE_NW)
    s = SintetizadorCasoNEWAVE(c, log)
    r = s.sintetiza_caso()
    assert r


def test_sintetizador_decomp():
    c = ArmazenadorCaso.recupera_caso(Configuracoes(),
                                      CAMINHO_TESTE_DCP)
    s = SintetizadorCasoDECOMP(c, log)
    r = s.sintetiza_caso()
    assert r
