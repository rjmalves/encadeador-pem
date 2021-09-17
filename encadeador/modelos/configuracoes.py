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
import re


class Configuracoes:
    """
    Configurações gerais para execução de um estudo
    encadeado NEWAVE/DECOMP.
    """
    def __init__(self):
        self._nome_estudo = None
        self._arquivo_lista_casos = None
        self._nome_diretorio_newave = None
        self._nome_diretorio_decomp = None

    @classmethod
    def le_variaveis_ambiente(cls) -> "Configuracoes":
        cb = BuilderConfiguracoesENV()
        c = cb.nome_estudo("NOME_ESTUDO")\
            .arquivo_lista_casos("ARQUIVO_LISTA_CASOS")\
            .nome_diretorio_newave("NOME_DIRETORIO_NEWAVE")\
            .nome_diretorio_decomp("NOME_DIRETORIO_DECOMP")\
            .build()
        return c

    @property
    def nome_estudo(self) -> str:
        """
        Nome do estudo encadeado, considerado para os logs.
        """
        return self._nome_estudo

    @property
    def arquivo_lista_casos(self) -> str:
        """
        Arquivo que contém a lista de casos a serem encadeados.
        """
        return self._arquivo_lista_casos

    @property
    def nome_diretorio_newave(self) -> str:
        """
        Nome do diretório que contém os casos de NEWAVE.
        """
        return self._nome_diretorio_newave

    @property
    def nome_diretorio_decomp(self) -> str:
        """
        Nome do diretório que contém os casos de NEWAVE.
        """
        return self._nome_diretorio_decomp


class BuilderConfiguracoes:
    """
    """
    def __init__(self,
                 configuracoes: Configuracoes = Configuracoes()):
        self._configuracoes = configuracoes

    def build(self) -> Configuracoes:
        return self._configuracoes

    @abstractmethod
    def nome_estudo(self, nome: str):
        pass

    @abstractmethod
    def arquivo_lista_casos(self, arquivo: str):
        pass

    @abstractmethod
    def nome_diretorio_newave(self, diretorio: str):
        pass

    @abstractmethod
    def nome_diretorio_decomp(self, diretorio: str):
        pass


class BuilderConfiguracoesENV(BuilderConfiguracoes):
    """
    """
    regex_alfanum = r"^([\w\/\\:.\-])+$"
    def __init__(self, configuracoes=Configuracoes()) -> None:
        super().__init__(configuracoes=configuracoes)

    @staticmethod
    def __le_e_confere_variavel(variavel: str):
        # Lê a variável de ambiente do nome do estudo
        valor = getenv(variavel)
        # Valida o conteúdo do nome do estudo
        if valor is None:
            raise ValueError(f"Variável {variavel} não encontrada")
        return valor

    def nome_estudo(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere tamanho != 0
        if len(valor) == 0:
            raise ValueError(f"Valor da variável {variavel} inválido: {valor}")
        self._configuracoes._nome_estudo = valor
        # Fluent method
        return self

    def arquivo_lista_casos(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se existe o arquivo no diretorio raiz de encadeamento
        if not isfile(join(curdir, valor)):
            raise FileNotFoundError("Arquivo com os casos não " +
                                    f"encontrado: {valor}")
        self._configuracoes._arquivo_lista_casos = valor
        # Fluent method
        return self

    def nome_diretorio_newave(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se existe o arquivo no diretorio raiz de encadeamento
        if not re.match(BuilderConfiguracoesENV.regex_alfanum, valor):
             raise ValueError(f"Nome de diretório {valor} inválido")
        self._configuracoes._nome_diretorio_newave = valor
        # Fluent method
        return self

    def nome_diretorio_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se existe o arquivo no diretorio raiz de encadeamento
        if not re.match(BuilderConfiguracoesENV.regex_alfanum, valor):
             raise ValueError(f"Nome de diretório {valor} inválido")
        self._configuracoes._nome_diretorio_decomp = valor
        # Fluent method
        return self
