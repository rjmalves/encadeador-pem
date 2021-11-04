from os.path import join
from dotenv import load_dotenv

import pytest

from encadeador.modelos.configuracoes import Configuracoes

DIRETORIO_TESTE = "tests/_arquivos/configuracoes"


def test_configuracoes_validas():
    load_dotenv(join(DIRETORIO_TESTE,
                     "valido.cfg"),
                override=True)
    c = Configuracoes.le_variaveis_ambiente()
    assert c.nome_estudo == "Estudo de Teste"
    assert c.arquivo_lista_casos == "main.py"
    assert c.nome_diretorio_newave == "newave"
    assert c.nome_diretorio_decomp == "decomp"
    assert c.diretorio_instalacao_newaves == "/home/USER/gmc2/versoes"
    assert c.diretorio_instalacao_decomps == "/home/USER/gmc2/versoes/DECOMP"
    assert c.gerenciador_fila == "PBS"
    assert c.versao_newave == "27.0.1"
    assert c.versao_decomp == "30.1"
    assert c.variaveis_encadeadas == ["EARM","TVIAGEM","GNL"]
    assert c.flexibiliza_deficit == 1
    assert c.maximo_flexibilizacoes_revisao == 30
    assert c.ultimas_iteracoes_flexibilizacao == 0
    assert c.metodo_flexibilizacao == "absoluto"
    assert c.adequa_decks_newave
    assert c.cvar == [50, 35]
    assert c.opcao_parpa == [3, 0]
    assert c.adequa_decks_decomp
    assert c.previne_gap_negativo
    assert c.maximo_iteracoes_decomp == 500
    assert c.fator_aumento_gap_decomp == 0.01
    assert c.gap_maximo_decomp == 0.1
    assert c.processadores_no == 72
    assert c.processadores_minimos_newave==1
    assert c.processadores_maximos_newave==1
    assert c.processadores_minimos_decomp==1
    assert c.processadores_maximos_decomp==1
    assert c.ajuste_processadores_newave==0
    assert c.ajuste_processadores_decomp==0
    

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

def test_metodo_flexibilizacao():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_metodo_flexibilizacao.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_adequa_decks_newave():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_adequa_decks_newave.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_cvar():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_cvar.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_opcao_parpa():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_opcao_parpa.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_adequa_decks_decomp():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_adequa_decks_decomp.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_previne_gap_negativo():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_previne_gap_negativo.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_maximo_iteracoes_decomp():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_maximo_iteracoes_decomp.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_fator_aumento_gap():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_fator_aumento_gap.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_gap_maximo():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_gap_maximo.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_diretorio_instalacao():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_diretorio_instalacao.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_processadores():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_processadores.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_ajuste_processadores():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_ajuste_processadores.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_variaveis_encadeadas():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_variaveis_encadeadas.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()

def test_script_converte_codificacao():
    with pytest.raises(ValueError):
        load_dotenv(join(DIRETORIO_TESTE,
                         "invalido_script_converte.cfg"),
                    override=True)
        Configuracoes.le_variaveis_ambiente()
