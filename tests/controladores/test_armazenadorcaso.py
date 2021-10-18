import logging
import pytest
import time
from os.path import join
from typing import List
from pytest_mock.plugin import MockerFixture

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.modelos.caso import CasoDECOMP
from encadeador.modelos.estadojob import EstadoJob
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso

CAMINHO_TESTE = join("tests", "_arquivos", "casos")
CAMINHO_TESTE_VAL = join("tests", "_arquivos", "casos", "valido")
CAMINHO_TESTE_INVAL = join("tests", "_arquivos", "casos", "invalido")
ANO_TESTE = 2021
MES_TESTE = 1
REVISAO_TESTE = 0
log = logging.getLogger()


def test_armazenador_caso_nao_inicializado():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        a = ArmazenadorCaso(c, log)
        a.armazena_caso()


def test_armazenador_newave_inicializado():
    c = CasoNEWAVE()
    c.configura_caso(CAMINHO_TESTE,
                     ANO_TESTE,
                     MES_TESTE,
                     REVISAO_TESTE,
                     Configuracoes())
    a = ArmazenadorCaso(c, log)
    a.armazena_caso()


def test_armazenador_newave_valido():
    a = ArmazenadorCaso.recupera_caso(Configuracoes(),
                                      CAMINHO_TESTE_VAL)


def test_armazenador_newave_invalido():
    with pytest.raises(KeyError):
        a = ArmazenadorCaso.recupera_caso(Configuracoes(),
                                          CAMINHO_TESTE_INVAL)
