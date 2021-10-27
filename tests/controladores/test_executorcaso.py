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
from encadeador.controladores.executorcaso import ExecutorCaso


DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests", "_arquivos", "casos")

CAMINHO_TESTE_NW = join(DIR_TESTE, "2021_01_rv0", "newave")
CAMINHO_TESTE_DCP = join(DIR_TESTE, "2021_01_rv0", "decomp")
log = logging.getLogger()


def test_executor_caso_nao_inicializado():
    chdir(DIR_TESTE)
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        e = ExecutorCaso.factory(c, log)
        e.executa_e_monitora_caso(None, None)
    chdir(DIR_INICIAL)


def test_executor_primeiro_newave_sucesso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.verifica_cortes_extraidos",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorNEWAVE.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    e.executa_e_monitora_caso(None, None)
    chdir(DIR_INICIAL)


def test_executor_newave_sucesso_nao_deleta_cortes(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    ca = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_DCP)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.verifica_cortes_extraidos",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorNEWAVE.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    e.executa_e_monitora_caso(None, ca)
    chdir(DIR_INICIAL)


def test_executor_newave_sucesso_deleta_cortes(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    ca = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_DCP)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.verifica_cortes_extraidos",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.deleta_cortes",
                 return_value=None)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorNEWAVE.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    e.executa_e_monitora_caso(None, ca)
    chdir(DIR_INICIAL)



def test_executor_newave_erro_prepara_caso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    ca = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_DCP)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.verifica_cortes_extraidos",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.prepara_caso",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorNEWAVE.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(None, ca)
    chdir(DIR_INICIAL)


def test_executor_newave_erro_encadeia_variaveis(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    ca = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_DCP)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.verifica_cortes_extraidos",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.encadeia_variaveis",
                 return_value=False)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorNEWAVE.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(None, ca)
    chdir(DIR_INICIAL)


def test_executor_newave_erro_executa_caso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    ca = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_DCP)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.verifica_cortes_extraidos",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorNEWAVE.executa_caso",
                 return_value=False)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(None, ca)
    chdir(DIR_INICIAL)


def test_executor_newave_erro_sintetiza_caso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    ca = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_DCP)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.verifica_cortes_extraidos",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorNEWAVE.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=False)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(None, ca)
    chdir(DIR_INICIAL)


def test_executor_newave_erro_armazena_caso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    ca = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_DCP)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_NW)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.executorcaso" +
                 ".SintetizadorNEWAVE.verifica_cortes_extraidos",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorNEWAVE.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorNEWAVE.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorNEWAVE.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=False)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(None, ca)
    chdir(DIR_INICIAL)


def test_executor_primeiro_decomp_sucesso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    cc = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorDECOMP.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    e.executa_e_monitora_caso(cc, None)
    chdir(DIR_INICIAL)


def test_executor_decomp_meio_sucesso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    cc = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorDECOMP.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    e.executa_e_monitora_caso(cc, c)
    chdir(DIR_INICIAL)


def test_executor_decomp_falha_preparador_caso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    cc = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.prepara_caso",
                 return_value=False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorDECOMP.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(cc, c)
    chdir(DIR_INICIAL)


def test_executor_decomp_falha_encadeia_variaveis(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    cc = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.encadeia_variaveis",
                 return_value=False)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorDECOMP.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(cc, c)
    chdir(DIR_INICIAL)


def test_executor_decomp_falha_flexibilizador_caso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    cc = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    c.finaliza_caso(False)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorDECOMP.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.flexibilizadorcaso" +
                 ".FlexibilizadorDECOMP.flexibiliza",
                 return_value=False)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(cc, c)
    chdir(DIR_INICIAL)


def test_executor_decomp_falha_sintetizador_caso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    cc = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorDECOMP.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=False)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=True)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(cc, c)
    chdir(DIR_INICIAL)


def test_executor_decomp_falha_armazenador_caso(mocker: MockerFixture):
    chdir(DIR_TESTE)
    load_dotenv("encadeia.cfg", override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    cc = ArmazenadorCaso.recupera_caso(cfg,
                                       CAMINHO_TESTE_NW)
    c = ArmazenadorCaso.recupera_caso(cfg,
                                      CAMINHO_TESTE_DCP)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.prepara_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.preparadorcaso" +
                 ".PreparadorDECOMP.encadeia_variaveis",
                 return_value=True)
    mocker.patch("encadeador.controladores.monitorcaso" +
                 ".MonitorDECOMP.executa_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.sintetizadorcaso" +
                 ".SintetizadorDECOMP.sintetiza_caso",
                 return_value=True)
    mocker.patch("encadeador.controladores.armazenadorcaso" +
                 ".ArmazenadorCaso.armazena_caso",
                 return_value=False)
    e = ExecutorCaso.factory(c, log)
    with pytest.raises(RuntimeError):
        e.executa_e_monitora_caso(cc, c)
    chdir(DIR_INICIAL)
