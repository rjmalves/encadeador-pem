from dotenv import load_dotenv
import pathlib
from os import chdir
from os.path import join
from logging import getLogger

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.modelos.caso import CasoDECOMP
from encadeador.modelos.arvorecasos import ArvoreCasos

DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests", "_arquivos", "casos")


def constroi_arvore_casos_teste() -> ArvoreCasos:
    log = getLogger()
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    return ArvoreCasos(cfg, log)


def test_arvorecasos_le_arquivos_casos():
    chdir(DIR_TESTE)
    a = constroi_arvore_casos_teste()
    a.le_arquivo_casos()
    chdir(DIR_INICIAL)
    assert a._diretorios_revisoes == [
        "2021_01_rv0",
        "2021_01_rv1",
        "2021_01_rv2",
        "2021_01_rv3",
    ]
    assert a._diretorios_casos == [
        join(DIR_TESTE, "2021_01_rv0", "newave"),
        join(DIR_TESTE, "2021_01_rv0", "decomp"),
        join(DIR_TESTE, "2021_01_rv1", "decomp"),
        join(DIR_TESTE, "2021_01_rv2", "decomp"),
        join(DIR_TESTE, "2021_01_rv3", "decomp"),
    ]


def test_arvorecasos_constroi_casos():
    chdir(DIR_TESTE)
    a = constroi_arvore_casos_teste()
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    assert r
    assert len(a.casos) == 5
    assert type(a.casos[0]) == CasoNEWAVE
    assert all([type(c) == CasoDECOMP for c in a.casos[1:]])


def test_arvorecasos_proximo_caso():
    chdir(DIR_TESTE)
    a = constroi_arvore_casos_teste()
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.proximo_caso
    assert isinstance(c, CasoDECOMP)
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 1


def test_arvorecasos_proximo_newave():
    chdir(DIR_TESTE)
    a = constroi_arvore_casos_teste()
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.proximo_newave
    assert c is None


def test_arvorecasos_proximo_decomp():
    chdir(DIR_TESTE)
    a = constroi_arvore_casos_teste()
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.proximo_decomp
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 1


def test_arvorecasos_ultimo_caso():
    chdir(DIR_TESTE)
    a = constroi_arvore_casos_teste()
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.ultimo_caso
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 0
    assert isinstance(c, CasoDECOMP)


def test_arvorecasos_ultimo_newave():
    chdir(DIR_TESTE)
    a = constroi_arvore_casos_teste()
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.ultimo_newave
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 0


def test_arvorecasos_ultimo_decomp():
    chdir(DIR_TESTE)
    a = constroi_arvore_casos_teste()
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.ultimo_decomp
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 0
