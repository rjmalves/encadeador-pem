import pathlib
from os import getenv, curdir
from os.path import isfile, join
from abc import abstractmethod
import re
from typing import List

from encadeador.utils.log import Log
from encadeador.utils.singleton import Singleton


class Configuracoes(metaclass=Singleton):
    """
    Configurações gerais para execução de um estudo
    encadeado NEWAVE/DECOMP.
    """

    def __init__(self):
        self._caminho_base_estudo = None
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
        self._ajuste_processadores_newave = None
        self._ajuste_processadores_decomp = None
        self._variaveis_encadeadas = None
        self._flexibiliza_deficit = None
        self._maximo_flexibilizacoes_revisao = None
        self._ultimas_iteracoes_flexibilizacao = None
        self._metodo_flexibilizacao = None
        self._adequa_decks_newave = None
        self._cvar = None
        self._opcao_parpa = None
        self._adequa_decks_decomp = None
        self._previne_gap_negativo = None
        self._maximo_iteracoes_decomp = None
        self._fator_aumento_gap_decomp = None
        self._gap_maximo_decomp = None
        self._script_converte_codificacao = None
        self._arquivo_regras_operacao_reservatorios = None

    @classmethod
    def le_variaveis_ambiente(cls) -> "Configuracoes":
        cb = BuilderConfiguracoesENV()
        var_ult_iter = "ULTIMAS_ITERACOES_PARA_FLEXIBILIZACAO"
        c = (
            cb.caminho_base_estudo()
            .nome_estudo("NOME_ESTUDO")
            .arquivo_lista_casos("ARQUIVO_LISTA_CASOS")
            .nome_diretorio_newave("NOME_DIRETORIO_NEWAVE")
            .nome_diretorio_decomp("NOME_DIRETORIO_DECOMP")
            .diretorio_instalacao_newaves("DIRETORIO_INSTALACAO_NEWAVES")
            .diretorio_instalacao_decomps("DIRETORIO_INSTALACAO_DECOMPS")
            .gerenciador_fila("GERENCIADOR_DE_FILA")
            .versao_newave("VERSAO_NEWAVE")
            .versao_decomp("VERSAO_DECOMP")
            .processadores_no("PROCESSADORES_POR_NO")
            .processadores_minimos_newave("PROCESSADORES_MINIMOS_NEWAVE")
            .processadores_maximos_newave("PROCESSADORES_MAXIMOS_NEWAVE")
            .processadores_minimos_decomp("PROCESSADORES_MINIMOS_DECOMP")
            .processadores_maximos_decomp("PROCESSADORES_MAXIMOS_DECOMP")
            .ajuste_processadores_newave("AJUSTE_PROCESSADORES_NEWAVE")
            .ajuste_processadores_decomp("AJUSTE_PROCESSADORES_DECOMP")
            .variaveis_encadeadas("VARIAVEIS_ENCADEADAS")
            .flexibiliza_deficit("FLEXIBILIZA_DEFICIT")
            .maximo_flexibilizacoes_revisao("MAXIMO_FLEXIBILIZACOES_REVISAO")
            .ultimas_iteracoes_flexibilizacao(var_ult_iter)
            .metodo_flexibilizacao("METODO_FLEXIBILIZACAO")
            .adequa_decks_newave("ADEQUA_DECKS_NEWAVE")
            .cvar("CVAR")
            .opcao_parpa("OPCAO_PARPA")
            .adequa_decks_decomp("ADEQUA_DECKS_DECOMP")
            .previne_gap_negativo("PREVINE_GAP_NEGATIVO")
            .maximo_iteracoes_decomp("MAXIMO_ITERACOES_DECOMP")
            .fator_aumento_gap_decomp("FATOR_AUMENTO_GAP_DECOMP")
            .gap_maximo_decomp("GAP_MAXIMO_DECOMP")
            .script_converte_codificacao("SCRIPT_CONVERTE_CODIFICACAO")
            .arquivo_regras_operacao_reservatorios(
                "ARQUIVO_REGRAS_OPERACAO_RESERVATORIOS"
            )
            .build()
        )
        return c

    @property
    def caminho_base_estudo(self) -> str:
        """
        Caminho absoluto até o diretório base em que o estudo
        é realizado.
        """
        return self._caminho_base_estudo

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
    def ajuste_processadores_newave(self) -> int:
        """
        Ajuste no número de processadores para modelo NEWAVE.
        """
        return self._ajuste_processadores_newave

    @property
    def ajuste_processadores_decomp(self) -> int:
        """
        Ajuste no número de processadores para modelo DECOMP.
        """
        return self._ajuste_processadores_decomp

    @property
    def variaveis_encadeadas(self) -> str:
        """
        Variáveis a serem encadeadas no estudo. Podem ser:

        - EARM (armazenamento inicial de todas as usinas),

        - TVIAGEM (tempo de viagem para usinas com tempo de viagem),

        - GNL (geração GNL para usinas com geração antecipada).
        """
        return self._variaveis_encadeadas

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
    def adequa_decks_newave(self) -> bool:
        """
        Opção de adequar os decks do NEWAVE antes de iniciar o estudo.
        São feitas as modificações de:

        - Parâmetros de CVAR utilizados (ALFA, LAMBDA)
        - Opção de PAR(p)-A utilizada (0 ou 3)

        :return: O uso, ou não, da adequação de decks para NEWAVE.
        :rtype: bool
        """
        return self._adequa_decks_newave

    @property
    def cvar(self) -> List[float]:
        """
        Parâmetros de CVAR usados para substituir no arquivo cvar.dat.

        :return: O par de opções (ALFA, LAMBDA) para uso no cvar.dat
        :rtype: List[float]
        """
        return self._cvar

    @property
    def opcao_parpa(self) -> List[int]:
        """
        Opção de PAR(p)-A utilizada para substituir no arquivo dger.dat e
        o uso, ou não, da redução automática da ordem.

        :return: O par de opções para uso no dger.dat.
        :rtype: List[int]
        """
        return self._opcao_parpa

    @property
    def adequa_decks_decomp(self) -> bool:
        """
        Opção de adequar os decks do DECOMP antes de iniciar o estudo.
        São feitas as modificações de:

        - Número máximo de iterações (registro NI)
        - Prevenção de gap negativo (registros RT), se habilitada

        :return: O uso, ou não, da adequação de decks para DECOMP.
        :rtype: bool
        """
        return self._adequa_decks_decomp

    @property
    def previne_gap_negativo(self) -> bool:
        """
        Habilita a prevenção, ou não, de gap negativo por meio da inserção
        dos registros RT com mnemônicos CRISTA e DESVIO no processo de
        adequação dos decks de DECOMP.

        :return: O uso ou não da prevenção de gap negativo.
        :rtype: bool
        """
        return self._previne_gap_negativo

    @property
    def maximo_iteracoes_decomp(self) -> int:
        """
        Número máximo de iterações do DECOMP.
        Alterar no DADGER de cada caso sempre que for rodar
        pela primeira vez (NI).
        """
        return self._maximo_iteracoes_decomp

    @property
    def fator_aumento_gap_decomp(self) -> int:
        """
        Fator de acréscimo no gap original.
        Alterar no DADGER caso tenha chegado ao limite de
        iterações sem convergir (GP).
        """
        return self._fator_aumento_gap_decomp

    @property
    def gap_maximo_decomp(self) -> int:
        """
        Valor máximo de gap de convergência do DECOMP.
        """
        return self._gap_maximo_decomp

    @property
    def script_converte_codificacao(self) -> str:
        """
        Caminho do script para converter os arquivos do diretorio para UTF-8.
        """
        return self._script_converte_codificacao

    @property
    def arquivo_regras_operacao_reservatorios(self) -> str:
        """
        Caminho do arquivo com as regras de operação dos reservatórios.
        """
        return self._arquivo_regras_operacao_reservatorios


class BuilderConfiguracoes:
    """ """

    def __init__(self, configuracoes: Configuracoes = Configuracoes()):
        self._configuracoes = configuracoes

    def build(self) -> Configuracoes:
        return self._configuracoes

    def caminho_base_estudo(self):
        self._configuracoes._caminho_base_estudo = pathlib.Path().resolve()
        return self

    @abstractmethod
    def nome_estudo(self, variavel: str):
        pass

    @abstractmethod
    def arquivo_lista_casos(self, variavel: str):
        pass

    @abstractmethod
    def nome_diretorio_newave(self, variavel: str):
        pass

    @abstractmethod
    def nome_diretorio_decomp(self, variavel: str):
        pass

    @abstractmethod
    def diretorio_instalacao_newaves(self, variavel: str):
        pass

    @abstractmethod
    def diretorio_instalacao_decomps(self, variavel: str):
        pass

    @abstractmethod
    def gerenciador_fila(self, variavel: str):
        pass

    @abstractmethod
    def versao_newave(self, variavel: str):
        pass

    @abstractmethod
    def versao_decomp(self, variavel: str):
        pass

    @abstractmethod
    def processadores_no(self, variavel: str):
        pass

    @abstractmethod
    def processadores_minimos_newave(self, variavel: str):
        pass

    @abstractmethod
    def processadores_maximos_newave(self, variavel: str):
        pass

    @abstractmethod
    def processadores_minimos_decomp(self, variavel: str):
        pass

    @abstractmethod
    def processadores_maximos_decomp(self, variavel: str):
        pass

    @abstractmethod
    def ajuste_processadores_newave(self, variavel: str):
        pass

    @abstractmethod
    def ajuste_processadores_decomp(self, variavel: str):
        pass

    @abstractmethod
    def variaveis_encadeadas(self, variavel: str):
        pass

    @abstractmethod
    def flexibiliza_deficit(self, variavel: str):
        pass

    @abstractmethod
    def maximo_flexibilizacoes_revisao(self, variavel: str):
        pass

    @abstractmethod
    def ultimas_iteracoes_flexibilizacao(self, variavel: str):
        pass

    @abstractmethod
    def metodo_flexibilizacao(self, variavel: str):
        pass

    @abstractmethod
    def adequa_decks_newave(self, variavel: str):
        pass

    @abstractmethod
    def cvar(self, variavel: str):
        pass

    @abstractmethod
    def opcao_parpa(self, variavel: str):
        pass

    @abstractmethod
    def adequa_decks_decomp(self, variavel: str):
        pass

    @abstractmethod
    def previne_gap_negativo(self, variavel: str):
        pass

    @abstractmethod
    def maximo_iteracoes_decomp(self, variavel: str):
        pass

    @abstractmethod
    def fator_aumento_gap_decomp(self, variavel: str):
        pass

    @abstractmethod
    def gap_maximo_decomp(self, variavel: str):
        pass

    @abstractmethod
    def script_converte_codificacao(self, variavel: str):
        pass

    @abstractmethod
    def arquivo_regras_operacao_reservatorios(self, variavel: str):
        pass


class BuilderConfiguracoesENV(BuilderConfiguracoes):
    """ """

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
            valor = int(variavel)
            valorfloat = float(variavel)
            if valor != valorfloat:
                raise ValueError()
        except ValueError:
            raise ValueError(f"Variável {variavel} não é inteira")
        return valor

    @staticmethod
    def __valida_float(variavel: str):
        try:
            valor = float(variavel)
        except ValueError:
            raise ValueError(f"Variável {variavel} não é real")
        return valor

    @staticmethod
    def __valida_bool(variavel: str):
        try:
            valor = int(variavel)
            if valor not in [0, 1]:
                raise ValueError()
            valor = bool(valor)
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
            raise FileNotFoundError(
                "Arquivo com os casos não " + f"encontrado: {valor}"
            )
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
        gerenciadores_validos = ["PBS", "SGE", "OGS"]
        if valor not in gerenciadores_validos:
            raise ValueError(
                f"Nome do gerenciador de filas {valor} "
                + "inválido. Válidos: PBS, SGE ou OGS."
            )
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

    def processadores_no(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor <= 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 1."
            )
        self._configuracoes._processadores_no = valor
        # Fluent method
        return self

    def processadores_minimos_newave(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor <= 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 1."
            )
        self._configuracoes._processadores_minimos_newave = valor
        # Fluent method
        return self

    def processadores_maximos_newave(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor <= 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 1."
            )
        self._configuracoes._processadores_maximos_newave = valor
        # Fluent method
        return self

    def processadores_minimos_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor <= 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 1."
            )
        self._configuracoes._processadores_minimos_decomp = valor
        # Fluent method
        return self

    def processadores_maximos_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor <= 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 1."
            )
        self._configuracoes._processadores_maximos_decomp = valor
        # Fluent method
        return self

    def ajuste_processadores_newave(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_bool(valor)
        self._configuracoes._ajuste_processadores_newave = valor
        # Fluent method
        return self

    def ajuste_processadores_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_bool(valor)
        self._configuracoes._ajuste_processadores_decomp = valor
        # Fluent method
        return self

    def variaveis_encadeadas(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se as variáveis está dentro das: GNL, TVIAGEM, EARM, ENA
        valor = valor.split(",")
        variaveis_validas = set(["", "GNL", "TVIAGEM", "EARM", "ENA"])
        if not set(valor).issubset(variaveis_validas):
            raise ValueError(
                f"Variáveis encadeadas informadas {valor}"
                + " são inválidas. "
                + " Válidas: EARM, TVIAGEM, GNL, ENA"
            )
        self._configuracoes._variaveis_encadeadas = valor
        # Fluent method
        return self

    def flexibiliza_deficit(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_bool(valor)
        self._configuracoes._flexibiliza_deficit = valor
        # Fluent method
        return self

    def maximo_flexibilizacoes_revisao(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor < 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 0."
            )
        self._configuracoes._maximo_flexibilizacoes_revisao = valor
        # Fluent method
        return self

    def ultimas_iteracoes_flexibilizacao(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor < 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 0."
            )
        self._configuracoes._ultimas_iteracoes_flexibilizacao = valor
        # Fluent method
        return self

    def metodo_flexibilizacao(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se é uma das possibilidades: absoluta ou percentual
        metodos_validos = ["absoluto", "percentual"]
        if valor not in metodos_validos:
            raise ValueError(
                f"Método de flexibilização {valor} inválido"
                + ". Métodos válidos: absoluto ou percentual."
            )
        self._configuracoes._metodo_flexibilizacao = valor
        # Fluent method
        return self

    def adequa_decks_newave(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_bool(valor)
        self._configuracoes._adequa_decks_newave = valor
        # Fluent method
        return self

    def cvar(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = valor.split(",")
        # Verifica se todos os valores são numéricos
        if len(valor) != 2:
            raise ValueError(
                "Devem ser informados apenas 2 valores "
                + f" como parâmetros de CVAR, não {len(valor)}."
            )
        if not all([v.replace(".", "0").isnumeric() for v in valor]):
            raise ValueError(
                "Devem ser informados parâmetros numéricos"
                + f" para o CVAR, não {valor}."
            )
        self._configuracoes._cvar = [float(v) for v in valor]
        # Fluent method
        return self

    def opcao_parpa(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = valor.split(",")
        # Verifica se todos os valores são inteiros
        if len(valor) != 2:
            raise ValueError(
                "Devem ser informados apenas 2 valores "
                + f" como opção de PAR(p)-A, não {len(valor)}."
            )
        if not all([v.isnumeric() for v in valor]):
            raise ValueError(
                "Devem ser informados parâmetros inteiros"
                + f" para o PAR(p)-A, não {valor}."
            )
        self._configuracoes._opcao_parpa = [int(v) for v in valor]
        # Fluent method
        return self

    def adequa_decks_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_bool(valor)
        self._configuracoes._adequa_decks_decomp = valor
        # Fluent method
        return self

    def previne_gap_negativo(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_bool(valor)
        self._configuracoes._previne_gap_negativo = valor
        # Fluent method
        return self

    def maximo_iteracoes_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0 e inferior a 999 (máximo da versão 30.16)
        if (valor < 0) or (valor > 999):
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro entre 1 e 999."
            )
        self._configuracoes._maximo_iteracoes_decomp = valor
        # Fluent method
        return self

    def fator_aumento_gap_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_float(valor)
        if valor < 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser do tipo float maior ou igual a 0."
            )
        self._configuracoes._fator_aumento_gap_decomp = valor
        # Fluent method
        return self

    def gap_maximo_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_float(valor)
        # Conferir se é >= 0 e inferior a 500 (máximo da versão 30.13)
        if valor < 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser do tipo float maior ou igual a 0."
            )
        self._configuracoes._gap_maximo_decomp = valor
        # Fluent method
        return self

    def script_converte_codificacao(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not re.match(BuilderConfiguracoesENV.regex_alfanum, valor):
            raise ValueError(f"Nome de arquivo {valor} inválido.")
        self._configuracoes._script_converte_codificacao = valor
        # Fluent method
        return self

    def arquivo_regras_operacao_reservatorios(self, variavel: str):
        valor = getenv(variavel)
        # Confere se existe o arquivo no diretorio raiz de encadeamento
        if valor is not None:
            if not isfile(join(curdir, valor)):
                Log.log().warning(
                    "Arquivo com as regras de operação de reservatórios não "
                    + f"encontrado: {valor}"
                )
        self._configuracoes._arquivo_regras_operacao_reservatorios = valor
        # Fluent method
        return self
