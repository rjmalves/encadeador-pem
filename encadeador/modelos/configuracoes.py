import pathlib
from os import getenv, curdir
from os.path import isfile, join
from abc import abstractmethod
import re
from typing import List
import validators

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
        self._versao_newave = None
        self._versao_decomp = None
        self._processadores_newave = None
        self._processadores_decomp = None
        self._variaveis_encadeadas_newave = None
        self._variaveis_encadeadas_decomp = None
        self._maximo_flexibilizacoes_revisao = None
        self._adequa_decks_newave = None
        self._cvar = None
        self._adequa_decks_decomp = None
        self._maximo_iteracoes_decomp = None
        self._gap_maximo_decomp = None
        self._formato_armazenamento_dados = None
        self._diretorio_sintese = None
        self._formato_sintese = None
        self._script_converte_codificacao = None
        self._arquivo_regras_operacao_reservatorios = None
        self._arquivo_regras_flexibilizacao_inviabilidades = None
        self._model_api = None
        self._result_api = None
        self._encadeador_service = None
        self._flexibilizador_service = None
        self._regras_reservatorios_service = None

    @classmethod
    def le_variaveis_ambiente(cls) -> "Configuracoes":
        cb = BuilderConfiguracoesENV()
        c = (
            cb.caminho_base_estudo()
            .nome_estudo("NOME_ESTUDO")
            .nome_diretorio_newave("NOME_DIRETORIO_NEWAVE")
            .nome_diretorio_decomp("NOME_DIRETORIO_DECOMP")
            .versao_newave("VERSAO_NEWAVE")
            .versao_decomp("VERSAO_DECOMP")
            .processadores_newave("PROCESSADORES_NEWAVE")
            .processadores_decomp("PROCESSADORES_DECOMP")
            .variaveis_encadeadas_newave("VARIAVEIS_ENCADEADAS_NEWAVE")
            .variaveis_encadeadas_decomp("VARIAVEIS_ENCADEADAS_DECOMP")
            .maximo_flexibilizacoes_revisao("MAXIMO_FLEXIBILIZACOES_REVISAO")
            .adequa_decks_newave("ADEQUA_DECKS_NEWAVE")
            .cvar("CVAR")
            .adequa_decks_decomp("ADEQUA_DECKS_DECOMP")
            .maximo_iteracoes_decomp("MAXIMO_ITERACOES_DECOMP")
            .gap_maximo_decomp("GAP_MAXIMO_DECOMP")
            .script_converte_codificacao("SCRIPT_CONVERTE_CODIFICACAO")
            .formato_armazenamento_dados("FORMATO_ARMAZENAMENTO_DADOS")
            .diretorio_sintese("DIRETORIO_SINTESE")
            .formato_sintese("FORMATO_SINTESE")
            .arquivo_lista_casos("ARQUIVO_LISTA_CASOS")
            .arquivo_regras_operacao_reservatorios(
                "ARQUIVO_REGRAS_OPERACAO_RESERVATORIOS"
            )
            .arquivo_regras_flexibilizacao_inviabilidades(
                "ARQUIVO_REGRAS_FLEXIBILIZACAO_INVIABILIDADES"
            )
            .model_api("MODEL_API")
            .result_api("RESULT_API")
            .encadeaor_service("ENCADEADOR_SERVICE")
            .flexibilizador_service("FLEXIBILIZADOR_SERVICE")
            .regras_reservatorios_service("REGRAS_RESERVATORIOS_SERVICE")
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
    def processadores_newave(self) -> int:
        """
        Número de processadores para modelo NEWAVE.
        """
        return self._processadores_newave

    @property
    def processadores_decomp(self) -> int:
        """
        Número de processadores para modelo DECOMP.
        """
        return self._processadores_decomp

    @property
    def variaveis_encadeadas_newave(self) -> str:
        """
        Variáveis a serem encadeadas para NEWAVE no estudo. Podem ser:

            - VARM (armazenamento inicial de todas as usinas),
            - GNL (geração GNL para usinas com geração antecipada).

        """
        return self._variaveis_encadeadas_newave

    @property
    def variaveis_encadeadas_decomp(self) -> str:
        """
        Variáveis a serem encadeadas para DECOMP no estudo. Podem ser:

            - VARM (armazenamento inicial de todas as usinas),
            - TVIAGEM (tempo de viagem da água entre usinas).
            - GNL (geração GNL para usinas com geração antecipada).

        """
        return self._variaveis_encadeadas_decomp

    @property
    def maximo_flexibilizacoes_revisao(self) -> int:
        """
        Número máximo de flexibilizações por revisão.
        """
        return self._maximo_flexibilizacoes_revisao

    @property
    def adequa_decks_newave(self) -> bool:
        """
        Opção de adequar os decks do NEWAVE antes de iniciar o estudo.
        São feitas as modificações de:

        - Parâmetros de CVAR utilizados (ALFA, LAMBDA)

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
    def adequa_decks_decomp(self) -> bool:
        """
        Opção de adequar os decks do DECOMP antes de iniciar o estudo.
        São feitas as modificações de:

        - Número máximo de iterações (registro NI)

        :return: O uso, ou não, da adequação de decks para DECOMP.
        :rtype: bool
        """
        return self._adequa_decks_decomp

    @property
    def maximo_iteracoes_decomp(self) -> int:
        """
        Número máximo de iterações do DECOMP.
        Alterar no DADGER de cada caso sempre que for rodar
        pela primeira vez (NI).
        """
        return self._maximo_iteracoes_decomp

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
    def formato_armazenamento_dados(self) -> str:
        """
        Configuração do formato de armazenamento de dados utilizado.
        """
        return self._formato_armazenamento_dados

    @property
    def diretorio_sintese(self) -> str:
        """
        Configuração do nome do diretório utilizado para síntese.
        """
        return self._diretorio_sintese

    @property
    def formato_sintese(self) -> str:
        """
        Configuração do formato de saída utilizado para a síntese.
        """
        return self._formato_sintese

    @property
    def arquivo_regras_operacao_reservatorios(self) -> str:
        """
        Caminho do arquivo com as regras de operação dos reservatórios.
        """
        return self._arquivo_regras_operacao_reservatorios

    @property
    def arquivo_regras_flexibilizacao_inviabilidades(self) -> str:
        """
        Caminho do arquivo com as regras de flexibilização das inviabilidades.
        """
        return self._arquivo_regras_flexibilizacao_inviabilidades

    @property
    def model_api(self) -> str:
        """
        URL para uso da hpc-model-api
        """
        return self._model_api

    @property
    def result_api(self) -> str:
        """
        URL para uso da result-api
        """
        return self._result_api

    @property
    def encadeador_service(self) -> str:
        """
        URL para uso do encadeador-service
        """
        return self._encadeador_service

    @property
    def flexibilizador_service(self) -> str:
        """
        URL para uso do flexibilizador-service
        """
        return self._flexibilizador_service

    @property
    def regras_reservatorios_service(self) -> str:
        """
        URL para uso do regras_reservatorios-service
        """
        return self._regras_reservatorios_service


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
        raise NotImplementedError()

    @abstractmethod
    def nome_diretorio_newave(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def nome_diretorio_decomp(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def versao_newave(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def versao_decomp(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def processadores_newave(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def processadores_decomp(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def variaveis_encadeadas_newave(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def variaveis_encadeadas_decomp(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def maximo_flexibilizacoes_revisao(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def adequa_decks_newave(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def cvar(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def adequa_decks_decomp(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def maximo_iteracoes_decomp(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def gap_maximo_decomp(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def script_converte_codificacao(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def formato_armazenamento_dados(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def diretorio_sintese(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def formato_sintese(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def arquivo_lista_casos(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def arquivo_regras_operacao_reservatorios(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def arquivo_regras_flexibilizacao_inviabilidades(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def model_api(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def result_api(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def encadeador_service(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def flexibilizador_service(self, variavel: str):
        raise NotImplementedError()

    @abstractmethod
    def regras_reservatorios_service(self, variavel: str):
        raise NotImplementedError()


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

    def processadores_newave(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor <= 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 1."
            )
        self._configuracoes._processadores_newave = valor
        # Fluent method
        return self

    def processadores_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_int(valor)
        # Conferir se é >= 0
        if valor <= 0:
            raise ValueError(
                f"Valor da variável {variavel} informada"
                + " deve ser inteiro maior ou igual a 1."
            )
        self._configuracoes._processadores_decomp = valor
        # Fluent method
        return self

    def variaveis_encadeadas_newave(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se as variáveis está dentro das: GNL, TVIAGEM, VARM, ENA
        valor = valor.split(",")
        variaveis_validas = set(["", "GNL", "VARM", "ENA"])
        if not set(valor).issubset(variaveis_validas):
            raise ValueError(
                f"Variáveis encadeadas (NEWAVE) informadas {valor}"
                + " são inválidas. "
                + " Válidas: VARM, GNL, ENA"
            )
        self._configuracoes._variaveis_encadeadas_newave = valor
        # Fluent method
        return self

    def variaveis_encadeadas_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se as variáveis está dentro das: GNL, TVIAGEM, VARM, ENA
        valor = valor.split(",")
        variaveis_validas = set(["", "GNL", "TVIAGEM", "VARM"])
        if not set(valor).issubset(variaveis_validas):
            raise ValueError(
                f"Variáveis encadeadas (DECOMP) informadas {valor}"
                + " são inválidas. "
                + " Válidas: VARM, TVIAGEM, GNL"
            )
        self._configuracoes._variaveis_encadeadas_decomp = valor
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

    def adequa_decks_decomp(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        valor = BuilderConfiguracoesENV.__valida_bool(valor)
        self._configuracoes._adequa_decks_decomp = valor
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

    def formato_armazenamento_dados(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(
            variavel
        ).strip()
        # Confere se as variáveis está dentro das: JSON, SQL
        variaveis_validas = set(["JSON", "SQL"])
        if valor in variaveis_validas:
            raise ValueError(
                f"Modo de armazenamento informado {valor}"
                + " é inválido. "
                + " Válidos: JSON, SQL"
            )
        self._configuracoes._formato_armazenamento_dados = valor
        # Fluent method
        return self

    def diretorio_sintese(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se a variável é válida
        if not re.match(BuilderConfiguracoesENV.regex_alfanum, valor):
            raise ValueError(f"Nome de arquivo {valor} inválido.")
        self._configuracoes._diretorio_sintese = valor
        # Fluent method
        return self

    def formato_sintese(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se a variável é válida
        variaveis_validas = set(["PARQUET", "CSV"])
        if valor in variaveis_validas:
            raise ValueError(
                f"Formato de síntese informado {valor}"
                + " é inválido. "
                + " Válidos: PARQUET, CSV"
            )
        self._configuracoes._formato_sintese = valor
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

    def arquivo_regras_flexibilizacao_inviabilidades(self, variavel: str):
        valor = getenv(variavel)
        # Confere se existe o arquivo no diretorio raiz de encadeamento
        if valor is not None:
            if not isfile(join(curdir, valor)):
                Log.log().warning(
                    "Arquivo com as regras de flexibilização das"
                    + f" inviabilidades não encontrado: {valor}"
                )
        self._configuracoes._arquivo_regras_flexibilizacao_inviabilidades = (
            valor
        )
        # Fluent method
        return self

    def model_api(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not validators.url(valor):
            raise ValueError(f"URL {valor} inválida.")
        self._configuracoes._model_api = valor
        # Fluent method
        return self

    def result_api(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not validators.url(valor):
            raise ValueError(f"URL {valor} inválida.")
        self._configuracoes._result_api = valor
        # Fluent method
        return self

    def encadeador_service(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not validators.url(valor):
            raise ValueError(f"URL {valor} inválida.")
        self._configuracoes._encadeador_service = valor
        # Fluent method
        return self

    def flexibilizador_service(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not validators.url(valor):
            raise ValueError(f"URL {valor} inválida.")
        self._configuracoes._flexibilizador_service = valor
        # Fluent method
        return self

    def regras_reservatorios_service(self, variavel: str):
        valor = BuilderConfiguracoesENV.__le_e_confere_variavel(variavel)
        # Confere se o caminho do diretorio é válido
        if not validators.url(valor):
            raise ValueError(f"URL {valor} inválida.")
        self._configuracoes._regras_reservatorios_service = valor
        # Fluent method
        return self
