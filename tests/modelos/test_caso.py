import time
import pytest

from encadeador.modelos.caso import Caso
from encadeador.modelos.caso import Configuracoes

CAMINHO_TESTE = "/home/user"
NOME_TESTE = "teste"


def test_caso_nao_configurado_nome():
    with pytest.raises(ValueError):
        c = Caso()
        c.nome


def test_caso_nao_configurado_caminho():
    with pytest.raises(ValueError):
        c = Caso()
        c.caminho


def test_caso_nao_configurado_configuracoes():
    with pytest.raises(ValueError):
        c = Caso()
        c.configuracoes


def test_caso_nao_configurado_tempo_fila():
    with pytest.raises(ValueError):
        c = Caso()
        c.tempo_fila


def test_caso_nao_configurado_tempo_execucao():
    with pytest.raises(ValueError):
        c = Caso()
        c.tempo_execucao


def test_caso_nao_configurado_numero_tentativas():
    with pytest.raises(ValueError):
        c = Caso()
        c.numero_tentativas


def test_caso_nao_configurado_sucesso():
    with pytest.raises(ValueError):
        c = Caso()
        c.sucesso


def test_configura_caso():
    c = Caso()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     Configuracoes())
    assert c.caminho == CAMINHO_TESTE
    assert c.nome == NOME_TESTE


def test_inicializa_parametros():
    c = Caso()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     Configuracoes())
    c.inicializa_parametros_execucao()
    assert c.tempo_fila == 0
    assert c.tempo_execucao == 0
    assert c.numero_tentativas == 0
    assert not c.sucesso


def test_reseta_parametros():
    c = Caso()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     Configuracoes())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    c.reseta_parametros_execucao()
    assert c.tempo_fila == 0
    assert c.tempo_execucao == 0
    assert c.numero_tentativas == 0
    assert not c.sucesso


def test_tempo_fila():
    c = Caso()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     Configuracoes())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    assert c.tempo_fila > 0
    c.inicia_caso()
    ti = c.tempo_fila
    time.sleep(1)
    assert c.tempo_fila == ti


def test_tempo_execucao():
    c = Caso()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     Configuracoes())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    c.inicia_caso()
    assert c.tempo_execucao > 0
    c.finaliza_caso(True)
    ti = c.tempo_execucao
    time.sleep(1)
    assert c.tempo_execucao == ti


def test_numero_tentativas():
    c = Caso()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     Configuracoes())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    c.inicia_caso()
    c.reseta_parametros_execucao()
    c.inicia_caso()
    assert c.numero_tentativas == 2


def test_sucesso():
    c = Caso()
    c.configura_caso(CAMINHO_TESTE,
                     NOME_TESTE,
                     Configuracoes())
    c.inicializa_parametros_execucao()
    c.coloca_caso_na_fila()
    c.inicia_caso()
    c.finaliza_caso(True)
    assert c.sucesso
