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
        self._diretorio_instalacao_newaves = None
        self._diretorio_instalacao_decomps = None
        self._gerenciador_fila = None
        self._versao_newave = None
        self._versao_decomp = None
        self._processadores_no = None
        self._processadores_minimos_newave = None
        self._processadores_maximos_newave = None
        self._processadores_minimos_decomp = None
        self._processadores_maximos_decomp = None
        self._flexibiliza_deficit = None
        self._maximo_flexibilizacoes_revisao = None
        self._ultimas_iteracoes_flexibilizacao = None
        self._metodo_flexibilizacao = None
        self._maximo_iteracoes_decomp = None
        self._fator_aumento_gap_decomp = None
        self._gap_maximo_decomp = None


    @classmethod
    def le_variaveis_ambiente(cls) -> "Configuracoes":
        cb = BuilderConfiguracoesENV()
        c = cb.nome_estudo("NOME_ESTUDO")\
            .arquivo_lista_casos("ARQUIVO_LISTA_CASOS")\
            .nome_diretorio_newave("NOME_DIRETORIO_NEWAVE")\
            .nome_diretorio_decomp("NOME_DIRETORIO_DECOMP")\
            .diretorio_instalacao_newaves("DIRETORIO_INSTALACAO_NEWAVES")\
            .diretorio_instalacao_decomps("DIRETORIO_INSTALACAO_DECOMPS")\
            .gerenciador_fila("GERENCIADOR_DE_FILA")\
            .versao_newave("VERSAO_NEWAVE")\
            .versao_decomp("VERSAO_DECOMP")\
            .processadores_no("PROCESSADORES_POR_NO")\
            .processadores_minimos_newave("PROCESSADORES_MINIMOS_NEWAVE")\
            .processadores_maximos_newave("PROCESSADORES_MAXIMOS_NEWAVE")\
            .processadores_minimos_decomp("PROCESSADORES_MINIMOS_DECOMP")\
            .processadores_maximos_decomp("PROCESSADORES_MAXIMOS_DECOMP")\
            .flexibiliza_deficit("FLEXIBILIZA_DEFICIT")\
            .maximo_flexibilizacoes_revisao("MAXIMO_FLEXIBILIZACOES_REVISAO")\
            .ultimas_iteracoes_flexibilizacao("ULTIMAS_ITERACOES_PARA_FLEXIBILIZACAO")\
            .metodo_flexibilizacao("METODO_FLEXIBILIZACAO")\
            .maximo_iteracoes_decomp("MAXIMO_ITERACOES_DECOMP")\
            .fator_aumento_gap_decomp("FATOR_AUMENTO_GAP_DECOMP")\
            .gap_maximo_decomp("GAP_MAXIMO_DECOMP")\
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
        Nome do diretório que contém os casos de NEWAVE no estudo encadeado.
        """
        return self._nome_diretorio_newave

    @property
    def nome_diretorio_decomp(self) -> str:
        """
        Nome do diretório que contém os casos de DECOMP no estudo encadeado.
        """
        return self._nome_diretorio_decomp

    @property
    def diretorio_instalacao_newaves(self) -> str:
        """
        Nome do diretório com a instalação das versões de NEWAVE.
        """
        return self._diretorio_instalacao_newaves

    @property
    def diretorio_instalacao_decomps(self) -> str:
        """
        Nome do diretório com a instalação das versões de DECOMP.
        """
        return self._diretorio_instalacao_decomps

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
    def processadores_no(self) -> int:
        """
        Número de processadores por nó.
        """
        return self._processadores_no

    @property
    def processadores_minimos_newave(self) -> int:
        """
        Número mínimo de processadores para modelo NEWAVE.
        """
        return self._processadores_minimos_newave

    @property
    def processadores_maximos_newave(self) -> int:
        """
        Número máximo de processadores para modelo NEWAVE.
        """
        return self._processadores_maximos_newave

    @property
    def processadores_minimos_decomp(self) -> int:
        """
        Número mínimo de processadores para modelo DECOMP.
        """
        return self._processadores_minimos_decomp

    @property
    def processadores_maximos_decomp(self) -> int:
        """
        Número máximo de processadores para modelo DECOMP.
        """
        return self._processadores_maximos_decomp

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
    
    @property
    def metodo_flexibilizacao(self) -> str:
        """
        Método de flexibilização: valor absoluto ou percentual.
        """
        return self._metodo_flexibilizacao

    @property
    def maximo_iteracoes_decomp(self) -> int:
        """
        Número máximo de iterações do DECOMP.
        Alterar no DADGER de cada caso sempre que for rodar pela primeira vez (NI).
        """
        return self._maximo_iteracoes_decomp

    @property
    def fator_aumento_gap_decomp(self) -> int:
        """
        Fator de acréscimo no gap original.
        Alterar no DADGER caso tenha chegado ao limite de iterações sem convergir (GP). 
        """
        return self._fator_aumento_gap_decomp

    @property
    def gap_maximo_decomp(self) -> int:
        """
        Valor máximo de gap de convergência do DECOMP.
        """
        return self._gap_maximo_decomp

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
    def diretorio_instalacao_newaves(self, diretorio: str):
        pass

    @abstractmethod
    def diretorio_instalacao_decomps(self, diretorio: str):
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
    def processadores_no(self, processadores: int):
        pass

    @abstractmethod
    def processadores_minimos_newave(self, processadores: int):
        pass

    @abstractmethod
    def processadores_maximos_newave(self, processadores: int):
        pass

    @abstractmethod
    def processadores_minimos_decomp(self, processadores: int):
        pass

    @abstractmethod
    def processadores_maximos_decomp(self, processadores: int):
        pass

    @abstractmethod
    def flexibiliza_deficit(self, flexibliza: int):
        pass

    @abstractmethod
    def maximo_flexibilizacoes_revisao(self, maximo: int):
        pass

    @abstractmethod
    def ultimas_iteracoes_flexibilizacao(self, iteracao: int):
        pass

    @abstractmethod
    def metodo_flexibilizacao(self, metodo: str):
        pass

    @abstractmethod
    def maximo_iteracoes_decomp(self, iteracoes: int):
        pass

    @abstractmethod
    def fator_aumento_gap_decomp(self, fator: float):
        pass

    @abstractmethod
    def gap_maximo_decomp(self, gap: float):
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

    @staticmethod  
    def __valida_int(variavel: str):
        try:
            valor=int(variavel)
            valorfloat=float(variavel)
            if valor != valorfloat:
                raise ValueError()
        except ValueError:
            raise ValueError(f"Variável {variavel} não é inteira")
        return valor

    @staticmethod 
    def __valida_float(variavel: str):
        try:
            valor=float(variavel)
        except ValueError:
            raise ValueError(f"Variável {variavel} não é real")
        return valor

    @staticmethod 
    def __valida_bool(variavel: str):
        try:
            valor=int(variavel)
            if valor not in [0,1]:
                raise ValueError()
            valor=bool(valor)
        except ValueError:
            raise ValueError(f"Variável {variavel} não é booleana")
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
    
    def diretorio_instalacao_newaves(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not re.match(BuilderConfiguracoesENV.regex_alfanum, valor):
             raise ValueError(f"Nome de diretório {valor} inválido.")
        self._configuracoes._diretorio_instalacao_newaves = valor
        # Fluent method
        return self

    def diretorio_instalacao_decomps(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not re.match(BuilderConfiguracoesENV.regex_alfanum, valor):
             raise ValueError(f"Nome de diretório {valor} inválido.")
        self._configuracoes._diretorio_instalacao_decomps = valor
        # Fluent method
        return self

    def gerenciador_fila(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se é uma das possibilidades: PBS, SGE ou OGS
        gerenciadores_validos = ["PBS","SGE","OGS"]
        if (valor not in gerenciadores_validos):
             raise ValueError(f"Nome do gerenciador de filas {valor} inválido. Gerenciadores válidos: PBS, SGE ou OGS.")
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

    def processadores_no(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if (valor <= 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 1.")
        self._configuracoes._processadores_no = valor
        # Fluent method
        return self

    def processadores_minimos_newave(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if (valor <= 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 1.")
        self._configuracoes._processadores_minimos_newave = valor
        # Fluent method
        return self

    def processadores_maximos_newave(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if (valor <= 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 1.")
        self._configuracoes._processadores_maximos_newave = valor
        # Fluent method
        return self
    
    def processadores_minimos_decomp(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if (valor <= 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 1.")
        self._configuracoes._processadores_minimos_decomp = valor
        # Fluent method
        return self

    def processadores_maximos_decomp(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if (valor <= 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 1.")
        self._configuracoes._processadores_maximos_decomp = valor
        # Fluent method
        return self

    def flexibiliza_deficit(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_bool(valor)
        self._configuracoes._flexibiliza_deficit = valor
        # Fluent method
        return self

    def maximo_flexibilizacoes_revisao(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if (valor < 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 0.")
        self._configuracoes._maximo_flexibilizacoes_revisao = valor
        # Fluent method
        return self

    def ultimas_iteracoes_flexibilizacao(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if (valor < 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro maior ou igual a 0.")
        self._configuracoes._ultimas_iteracoes_flexibilizacao = valor
        # Fluent method
        return self

    def metodo_flexibilizacao(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se é uma das possibilidades: absoluta ou percentual
        metodos_validos = ["absoluto","percentual"]
        if (valor not in metodos_validos):
             raise ValueError(f"Método de flexibilização {valor} inválido. Métodos válidos: absoluto ou percentual.")
        self._configuracoes._metodo_flexibilizacao = valor
        # Fluent method
        return self

    def maximo_iteracoes_decomp(self, variavel: int):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0 e inferior a 500 (máximo da versão 30.13)
        if (valor < 0) or (valor > 500):
            raise ValueError(f"Valor da variável {variavel} informada deve ser inteiro entre 1 e 500.")
        self._configuracoes._maximo_iteracoes_decomp = valor
        # Fluent method
        return self

    def fator_aumento_gap_decomp(self, variavel: float):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_float(valor)
        if (valor < 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser do tipo float maior ou igual a 0.")
        self._configuracoes._fator_aumento_gap_decomp = valor
        # Fluent method
        return self

    def gap_maximo_decomp(self, variavel: float):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_float(valor)
        # Conferir se é >= 0 e inferior a 500 (máximo da versão 30.13)
        if (valor < 0):
            raise ValueError(f"Valor da variável {variavel} informada deve ser do tipo float maior ou igual a 0.")
        self._configuracoes._gap_maximo_decomp = valor
        # Fluent method
        return self

