import logging
import pytest

from encadeador.modelos.caso import Configuracoes
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.controladores.preparadorcaso import PreparadorCasoNEWAVE
from encadeador.controladores.preparadorcaso import PreparadorCasoDECOMP

CAMINHO_TESTE_NW = "./tests/_arquivos/casos/2021_01_rv0/newave"
CAMINHO_TESTE_DCP = "./tests/_arquivos/casos/2021_01_rv0/decomp"
ANO_TESTE = 2021
MES_TESTE = 1
REVISAO_TESTE = 0
log = logging.getLogger()


def test_preparador_caso_nao_inicializado():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        p = PreparadorCasoNEWAVE(c)
        p.prepara_caso(log)


def test_preparador_newave():
    c = ArmazenadorCaso.recupera_caso(Configuracoes(),
                                      CAMINHO_TESTE_NW)
    p = PreparadorCasoNEWAVE(c)
    r = p.prepara_caso(log)
    assert r


def test_preparador_decomp():
    c_ant = ArmazenadorCaso.recupera_caso(Configuracoes(),
                                          CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(Configuracoes(),
                                      CAMINHO_TESTE_DCP)
    p = PreparadorCasoDECOMP(c)
    r = p.prepara_caso(log, caso_cortes=c_ant)
    assert r
