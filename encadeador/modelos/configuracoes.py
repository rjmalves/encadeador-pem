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
        self._gerenciador_fila = None
        self._versao_newave = None
        self._versao_decomp = None
        self._flexibiliza_deficit = None
        self._maximo_flexibilizacoes_revisao = None
        self._ultimas_iteracoes_flexibilizacao = None

    @classmethod
    def le_variaveis_ambiente(cls) -> "Configuracoes":
        cb = BuilderConfiguracoesENV()
        c = cb.nome_estudo("NOME_ESTUDO")\
            .arquivo_lista_casos("ARQUIVO_LISTA_CASOS")\
            .nome_diretorio_newave("NOME_DIRETORIO_NEWAVE")\
            .nome_diretorio_decomp("NOME_DIRETORIO_DECOMP")\
            .gerenciador_fila("GERENCIADOR_DE_FILA")\
            .versao_newave("VERSAO_NEWAVE")\
            .versao_decomp("VERSAO_DECOMP")\
            .flexibiliza_deficit("FLEXIBILIZA_DEFICIT")\
            .maximo_flexibilizacoes_revisao("MAXIMO_FLEXIBILIZACOES_REVISAO")\
            .ultimas_iteracoes_flexibilizacao("ULTIMAS_ITERACOES_PARA_FLEXIBILIZACAO")\
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

    @property
    def gerenciador_fila(self) -> str:
        """
        Gerenciador de fila a ser utilizado: PBS, SGE, OGS.
        """
        return self._gerenciador_fila

    @property
    def versao_newave(self) -> str:
        """
        Versão do modelo NEWAVE.
        """
        return self._versao_newave

    @property
    def versao_decomp(self) -> str:
        """
        Versão do modelo DECOMP.
        """
        return self._versao_decomp

    @property
    def flexibiliza_deficit(self) -> int:
        """
        Sinalização de flexibilização ou não de déficit.
        """
        return self._flexibiliza_deficit

    @property
    def maximo_flexibilizacoes_revisao(self) -> int:
        """
        Número máximo de flexibilizações por revisão.
        """
        return self._maximo_flexibilizacoes_revisao

    @property
    def ultimas_iteracoes_flexibilizacao(self) -> int:
        """
        Últimas iterações consideradas para flexibilização.
        """
        return self._ultimas_iteracoes_flexibilizacao

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

    @abstractmethod
    def gerenciador_fila(self, gerenciador: str):
        pass

    @abstractmethod
    def versao_newave(self, versao: str):
        pass

    @abstractmethod
    def versao_decomp(self, versao: str):
        pass

    @abstractmethod
    def flexibiliza_deficit(self, flexiblizacao: int):
        pass

    @abstractmethod
    def maximo_flexibilizacoes_revisao(self, max_flexiblizacoes: int):
        pass

    @abstractmethod
    def ultimas_iteracoes_flexibilizacao(self, ultimas_iteracoes_flexiblizacao: int):
        pass


class BuilderConfiguracoesENV(BuilderConfiguracoes):
    """
    """
    regex_alfanum = r"^([\w\/\\:.\-])+$"
    def __init__(self, configuracoes=Configuracoes()) -> None:
        super().__init__(configuracoes=configuracoes)

    @staticmethod
    def __le_e_confere_variavel(variavel: str):
        # Lê a variável de ambiente 
        valor = getenv(variavel)
        # Valida o conteúdo 
        if valor is None:
            raise ValueError(f"Variável {variavel} não encontrada")
        return valor

    @staticmethod # mariana 
    def __le_e_confere_variavel_int(variavel: int):
        # Lê a variável de ambiente do nome do estudo
        valor = int(getenv(variavel))
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
        # Confere se o caminho do diretorio é válido
        if not re.match(BuilderConfiguracoesENV.regex_alfanum, valor):
             raise ValueError(f"Nome de diretório {valor} inválido")
        self._configuracoes._nome_diretorio_newave = valor
        # Fluent method
        return self

    def nome_diretorio_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not re.match(BuilderConfiguracoesENV.regex_alfanum, valor):
             raise ValueError(f"Nome de diretório {valor} inválido")
        self._configuracoes._nome_diretorio_decomp = valor
        # Fluent method
        return self

    def gerenciador_fila(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se é uma das possibilidades: PBS, SGE ou OGS
        gerenciadores_validos = ["PBS","SGE","OGS"]
        if (valor not in gerenciadores_validos):
             raise ValueError(f"Nome do gerenciador de filas {valor} inválido. Deve ser PBS, SGE ou OGS.")
        self._configuracoes._gerenciador_fila = valor
        # Fluent method
        return self

    def versao_newave(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere tamanho != 0
        if len(valor) == 0:
            raise ValueError(f"Valor da variável {variavel} inválido: {valor}")
        self._configuracoes._versao_newave = valor
        # Fluent method
        return self
    
    def versao_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere tamanho != 0
        if len(valor) == 0:
            raise ValueError(f"Valor da variável {variavel} inválido: {valor}")
        self._configuracoes._versao_decomp = valor
        # Fluent method
        return self

    def flexibiliza_deficit(self, variavel: int):
            valor = BuilderConfiguracoesENV.__le_e_confere_variavel_int(variavel)
            # Conferir se é 0 (não) ou 1 (sim)
            if (valor not in [0,1]) or (not isinstance(valor,int)):
                raise ValueError(f"Valor da variável {variavel} informada deve ser 0 ou 1.")
            self._configuracoes._flexibiliza_deficit = valor
            # Fluent method
            return self

    def maximo_flexibilizacoes_revisao(self, variavel: int):
            valor = BuilderConfiguracoesENV.__le_e_confere_variavel_int(variavel)
            # Conferir se é >= 0
            if (valor < 0) or (not isinstance(valor,int)):
                raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 0.")
            self._configuracoes._maximo_flexibilizacoes_revisao = valor
            # Fluent method
            return self

    def ultimas_iteracoes_flexibilizacao(self, variavel: int):
            valor = BuilderConfiguracoesENV.__le_e_confere_variavel_int(variavel)
            # Conferir se é >= 0
            if (valor < 0) or (not isinstance(valor,int)):
                raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 0.")
            self._configuracoes._ultimas_iteracoes_flexibilizacao = valor
            # Fluent method
            return self