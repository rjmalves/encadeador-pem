import pytest
import pathlib
from os import chdir
from os.path import join
from logging import getLogger

from encadeador.modelos.caso import Configuracoes
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.modelos.caso import CasoDECOMP
from encadeador.modelos.arvorecasos import ArvoreCasos

DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests/_arquivos/casos")


def test_arvorecasos_le_arquivos_casos():
    log = getLogger()
    cfg = Configuracoes()
    chdir(DIR_TESTE)
    a = ArvoreCasos(Configuracoes(),
                    log)
    a.le_arquivo_casos()
    chdir(DIR_INICIAL)
    assert a._diretorios_revisoes == ["2021_01_rv0",
                                      "2021_01_rv1",
                                      "2021_01_rv2",
                                      "2021_01_rv3"]
    assert a._diretorios_casos == [join("2021_01_rv0", "newave"),
                                   join("2021_01_rv0", "decomp"),
                                   join("2021_01_rv1", "decomp"),
                                   join("2021_01_rv2", "decomp"),
                                   join("2021_01_rv3", "decomp")]


def test_arvorecasos_constroi_casos():
    log = getLogger()
    cfg = Configuracoes()
    chdir(DIR_TESTE)
    a = ArvoreCasos(Configuracoes(),
                    log)
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    assert r
    assert len(a.casos) == 5
    assert type(a.casos[0]) == CasoNEWAVE
    assert all([type(c) == CasoDECOMP for c in a.casos[1:]])


def test_arvorecasos_proximo_caso():
    log = getLogger()
    cfg = Configuracoes()
    chdir(DIR_TESTE)
    a = ArvoreCasos(Configuracoes(),
                    log)
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.proximo_caso
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 1
    assert isinstance(c, CasoDECOMP)


def test_arvorecasos_proximo_newave():
    log = getLogger()
    cfg = Configuracoes()
    chdir(DIR_TESTE)
    a = ArvoreCasos(Configuracoes(),
                    log)
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.proximo_newave
    assert c is None


def test_arvorecasos_proximo_decomp():
    log = getLogger()
    cfg = Configuracoes()
    chdir(DIR_TESTE)
    a = ArvoreCasos(Configuracoes(),
                    log)
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.proximo_decomp
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 1


def test_arvorecasos_ultimo_caso():
    log = getLogger()
    cfg = Configuracoes()
    chdir(DIR_TESTE)
    a = ArvoreCasos(Configuracoes(),
                    log)
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
    log = getLogger()
    cfg = Configuracoes()
    chdir(DIR_TESTE)
    a = ArvoreCasos(Configuracoes(),
                    log)
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.ultimo_newave
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 0


def test_arvorecasos_ultimo_decomp():
    log = getLogger()
    cfg = Configuracoes()
    chdir(DIR_TESTE)
    a = ArvoreCasos(Configuracoes(),
                    log)
    a.le_arquivo_casos()
    r = a.constroi_casos()
    chdir(DIR_INICIAL)
    c = a.ultimo_decomp
    assert c is not None
    assert c.ano == 2021
    assert c.mes == 1
    assert c.revisao == 0
