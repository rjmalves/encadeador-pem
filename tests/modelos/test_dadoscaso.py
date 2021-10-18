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
from encadeador.modelos.dadoscaso import DadosCaso


DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests", "_arquivos", "casos")

CAMINHO_TESTE_NW = join(DIR_TESTE, "2021_01_rv0", "newave")
CAMINHO_TESTE_DCP = join(DIR_TESTE, "2021_01_rv0", "decomp")
log = logging.getLogger()


def test_dadoscaso_existemdados_sucesso():
    chdir(DIR_TESTE)
    e = DadosCaso.existem_dados(CAMINHO_TESTE_NW)
    assert e
    chdir(DIR_INICIAL)


def test_dadoscaso_existemdados_erro():
    chdir(DIR_TESTE)
    e = DadosCaso.existem_dados(DIR_INICIAL)
    assert not e
    chdir(DIR_INICIAL)

    # c = ArmazenadorCaso.recupera_caso(cfg,
    #                                   CAMINHO_TESTE_NW)
