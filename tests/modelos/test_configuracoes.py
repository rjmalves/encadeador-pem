
from dotenv import load_dotenv

from modelos.configuracoes import BuilderConfiguracoesENV


def test_configuracoes_validas():
    load_dotenv("teste_configuracoes_valido.cfg", override=True)
    cb = BuilderConfiguracoesENV()
    c = cb\
        .nome_estudo("NOME_ESTUDO")\
        .arquivo_lista_casos("ARQUIVO_LISTA_CASOS").build()
    assert c.nome_estudo == "Estudo de Teste"
    assert c.arquivo_lista_casos == "main.py"


def test_configuracoes_invalidas():
    pass

