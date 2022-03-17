import time
import pytest
from os.path import join
from dotenv import load_dotenv

from encadeador.modelos.caso import Caso
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.modelos.caso import CasoDECOMP
from encadeador.modelos.configuracoes import Configuracoes

ARQ_CFG = join("tests", "_arquivos", "configuracoes", "valido.cfg")
CAMINHO_TESTE = "/home/user"
ANO_TESTE = 2021
MES_TESTE = 1
REVISAO_TESTE = 0


def constroi_caso_configurado_teste(caso: Caso) -> Caso:
    load_dotenv(ARQ_CFG, override=True)
    cfg = Configuracoes.le_variaveis_ambiente()
    caso.configura_caso(CAMINHO_TESTE, ANO_TESTE, MES_TESTE, REVISAO_TESTE, cfg)
    return caso


def test_caso_nao_configurado_nome():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        c.nome


def test_caso_nao_configurado_caminho():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        c.caminho


def test_caso_nao_configurado_configuracoes():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        c.configuracoes


def test_caso_nao_configurado_tempo_fila():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        c.tempo_fila


def test_caso_nao_configurado_tempo_execucao():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        c.tempo_execucao


def test_caso_nao_configurado_numero_tentativas():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        c.numero_tentativas


def test_caso_nao_configurado_sucesso():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        c.sucesso


def test_configura_caso():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    assert c.caminho == CAMINHO_TESTE


def test_inicializa_parametros():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    c.inicializa_parametros_execucao()
    assert c.tempo_fila == 0
    assert c.tempo_execucao == 0
    assert c.numero_tentativas == 0
    assert not c.sucesso


def test_reseta_parametros():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    c.reseta_parametros_execucao()
    assert c.tempo_fila == 0
    assert c.tempo_execucao == 0
    assert c.numero_tentativas == 0
    assert not c.sucesso


def test_tempo_fila():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    time.sleep(0.1)
    assert c.tempo_fila > 0
    c.inicia_caso()
    ti = c.tempo_fila
    time.sleep(0.1)
    assert c.tempo_fila == ti


def test_tempo_execucao():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    c.inicia_caso()
    time.sleep(0.1)
    assert c.tempo_execucao > 0
    c.finaliza_caso(True)
    ti = c.tempo_execucao
    time.sleep(0.1)
    assert c.tempo_execucao == ti


def test_numero_tentativas():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    c.inicia_caso()
    c.reseta_parametros_execucao()
    c.inicia_caso()
    assert c.numero_tentativas == 2


def test_sucesso():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    c.inicia_caso()
    c.finaliza_caso(True)
    assert c.sucesso


def test_caso_newave_nao_configurado():
    with pytest.raises(ValueError):
        c = CasoNEWAVE()
        c.ano
        c.mes
        c.revisao


def test_caso_newave_configurado():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    assert c.ano == ANO_TESTE
    assert c.mes == MES_TESTE


def test_caso_newave_numero_processadores():
    c = constroi_caso_configurado_teste(CasoNEWAVE())
    c.inicializa_parametros_execucao()
    assert c.numero_processadores == 1


def test_caso_decomp_nao_configurado():
    with pytest.raises(ValueError):
        c = CasoDECOMP()
        c.ano
        c.mes
        c.revisao


def test_caso_decomp_configurado():
    c = constroi_caso_configurado_teste(CasoDECOMP())
    assert c.ano == ANO_TESTE
    assert c.mes == MES_TESTE


def test_caso_decomp_numero_processadores():
    c = constroi_caso_configurado_teste(CasoDECOMP())
    c.inicializa_parametros_execucao()
    assert c.numero_processadores == 1
