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
