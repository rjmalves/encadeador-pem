from os.path import join
from dotenv import load_dotenv

import pytest

from encadeador.modelos.configuracoes import Configuracoes

DIRETORIO_TESTE = "tests/modelos/_arquivos"


def test_configuracoes_validas():
    load_dotenv(join(DIRETORIO_TESTE,
                     "valido.cfg"),
                override=True)
    c = Configuracoes.le_variaveis_ambiente()
    assert c.nome_estudo == "Estudo de Teste"
    assert c.arquivo_lista_casos == "main.py"
    assert c.nome_diretorio_newave == "newave"
    assert c.nome_diretorio_decomp == "decomp"
    assert c.gerenciador_fila == "PBS"
    assert c.versao_newave == "27.0.1"
    assert c.versao_decomp == "30.1"
    assert c.flexibiliza_deficit == 1
    assert c.maximo_flexibilizacoes_revisao == 30
    assert c.ultimas_iteracoes_flexibilizacao == 0

def test_nome_estudo_invalido():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_nome_estudo.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()


def test_arquivo_lista_casos_inexistente():
    with pytest.raises(FileNotFoundError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_arquivo_lista_casos.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_nome_diretorio_newave_invalido():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_nome_diretorio_newave.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_nome_diretorio_decomp_invalido():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_nome_diretorio_decomp.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_gerenciador_fila():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_gerenciador_fila.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_versoes():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_versoes.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_flexibiliza_deficit():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_flex_deficit.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_maximo_flebilizacoes_revisao():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_maximo_flexibilizacoes.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_ultimas_iteracoes_flexibilizacao():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_ultimas_iteracoes.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()