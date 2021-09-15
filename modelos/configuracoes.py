# Configurações do encadeador

# NOME_ESTUDO (str)
# ARQUIVO_LISTA_CASOS (str)

# DIRETORIO_NEWAVE (str)
# DIRETORIO_DECOMP (str)
# DIRETORIO_INSTALACAO_NEWAVES (str)
# DIRETORIO_INSTALACAO_DECOMPS (str)
# GERENCIADOR_FILA (str - PBS, SGE ou OGS)

# VERSAO_NEWAVE (str)
# VERSAO_DECOMP (str)
# PROCESSADORES_POR_NO (int >= 1)
# PROCESSADORES_MINIMOS_NEWAVE (int >= 1)
# PROCESSADORES_MAXIMOS_NEWAVE (int >= 1)
# PROCESSADORES_MINIMOS_DECOMP (int >= 1)
# PROCESSADORES_MAXIMOS_DECOMP (int >= 1)
# AJUSTE_PROCESSADORES_NEWAVE (bool)
# AJUSTE_PROCESSADORES_DECOMP (bool)
# VARIAVEIS_ENCADEADAS (list(str))
# FREQUENCIA_NEWAVE (str - SEMANAL ou MENSAL)

# FLEXIBILIZA_DEFICIT (bool)
# MAXIMO_FLEXIBILIZACOES_REVISAO (int >= 1)
# ULTIMAS_ITERACOES_FLEXIBILIZACAO (int >= 0)
# METODO_FLEXIBILIZACAO (str - ABSOLUTA ou PERCENTUAL)

# MAXIMO_ITERACOES_DECOMP (int >= 1)
# FATOR_AUMENTO_GAP_DECOMP (float >= 1)
# GAP_MAXIMO_DECOMP (float > 0)

# Builder

from os import getenv, curdir
from os.path import isfile, join
from abc import abstractmethod


class Configuracoes:
    """
    Configurações gerais para execução de um estudo
    encadeado NEWAVE/DECOMP.
    """
    def __init__(self):
        self.nome_estudo = None
        self.arquivo_lista_casos = None


class BuilderConfiguracoes:
    """
    """
    def __init__(self, configuracoes=Configuracoes()):
        self._configuracoes = configuracoes

    def build(self):
        return self._configuracoes

    @abstractmethod
    def nome_estudo(self, nome: str):
        pass

    @abstractmethod
    def arquivo_lista_casos(self, arquivo: str):
        pass


class BuilderConfiguracoesENV(BuilderConfiguracoes):
    """
    """
    def __init__(self, configuracoes=Configuracoes()) -> None:
        super().__init__(configuracoes=configuracoes)

    @staticmethod
    def __le_e_confere_variavel(variavel: str):
        # Lê a variável de ambiente do nome do estudo
        valor = getenv(variavel)
        # Valida o conteúdo do nome do estudo
        if valor == None:
            raise ValueError(f"Variável {variavel} não encontrada")
        return valor

    def nome_estudo(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere tamanho != 0
        if len(valor) == 0:
            raise ValueError(f"Valor da variável {variavel} inválido: {valor}")
        self._configuracoes.nome_estudo = valor
        # Fluent method
        return self

    def arquivo_lista_casos(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se existe o arquivo no diretorio raiz de encadeamento
        if not isfile(join(curdir, valor)):
            raise ValueError(f"Arquivo com os casos não encontrado: {valor}")
        # Armazena o conteúdo
        self._configuracoes.arquivo_lista_casos = valor
        # Fluent method
        return self


cb = BuilderConfiguracoesENV()
c = cb.\
    nome_estudo("NOME_ESTUDO").\
    arquivo_lista_casos("ARQUIVO_LISTA_CASOS").build()

c.arquivo_lista_casos