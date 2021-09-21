from abc import abstractmethod
import time

# STUB
class Configuracoes:
    def __init__(self) -> None:
        self.diretorio_newave = "newave"
        self.versao_newave = "newave"
        self.diretorio_instalacao_newaves = "./tests/_arquivos"
        self.processadores_minimos_newave = 0
        self.processadores_maximos_newave = 0
        self.ajuste_processadores_newave = False
        self.gerenciador_fila = "SGE"


class Caso:
    """
    Classe base que representa todos os casos executados no
    estudo encadeado. É responsável por armazenar informações
    associadas ao caso. Não lida com a sua execução, pré ou pós
    processamento dos decks.
    """
    def __init__(self,) -> None:
        self._caminho_caso: str = None  # type: ignore
        self._nome_caso: str = None  # type: ignore
        self._configuracoes: Configuracoes = None  # type: ignore
        self._instante_entrada_fila: float = None  # type: ignore
        self._instante_inicio_execucao: float = None  # type: ignore
        self._instante_fim_execucao: float = None  # type: ignore
        self._numero_tentativas: int = None  # type: ignore
        self._numero_processadores: int = None  # type: ignore
        self._sucesso: bool = None  # type: ignore

    def configura_caso(self,
                       caminho: str,
                       nome: str,
                       cfg: Configuracoes):
        self._caminho_caso = caminho
        self._nome_caso = nome
        self._configuracoes = cfg

    @abstractmethod
    def _obtem_numero_processadores(self) -> int:
        pass

    def reseta_parametros_execucao(self):
        self._instante_entrada_fila = 0
        self._instante_inicio_execucao = 0
        self._instante_fim_execucao = 0
        self._sucesso = False

    def inicializa_parametros_execucao(self):
        self.reseta_parametros_execucao()
        self._numero_tentativas = 0
        self._numero_processadores = self._obtem_numero_processadores()
        self._sucesso = False

    def coloca_caso_na_fila(self):
        self.reseta_parametros_execucao()
        self._instante_entrada_fila = time.time()

    def inicia_caso(self):
        self._instante_inicio_execucao = time.time()
        self._numero_tentativas += 1

    def finaliza_caso(self, sucesso: bool):
        self._sucesso = sucesso
        self._instante_fim_execucao = time.time()

    @staticmethod
    def _verifica_caso_configurado(valor):
        if valor is None:
            raise ValueError("Caso não configurado!")
        return valor

    @property
    def caminho(self) -> str:
        Caso._verifica_caso_configurado(self._caminho_caso)
        return self._caminho_caso

    @property
    def nome(self) -> str:
        Caso._verifica_caso_configurado(self._nome_caso)
        return self._nome_caso

    @property
    def configuracoes(self) -> Configuracoes:
        Caso._verifica_caso_configurado(self._configuracoes)
        return self._configuracoes

    @property
    def tempo_fila(self) -> float:
        Caso._verifica_caso_configurado(self._instante_entrada_fila)
        Caso._verifica_caso_configurado(self._instante_inicio_execucao)
        if self._instante_entrada_fila == 0:
            t_fila = 0.
        elif self._instante_inicio_execucao == 0:
            t_fila = time.time() - self._instante_entrada_fila
        else:
            t_fila = (self._instante_inicio_execucao -
                      self._instante_entrada_fila)
        return t_fila

    @property
    def tempo_execucao(self) -> float:
        Caso._verifica_caso_configurado(self._instante_entrada_fila)
        Caso._verifica_caso_configurado(self._instante_inicio_execucao)
        if self._instante_inicio_execucao == 0:
            t_exec = 0.
        elif self._instante_fim_execucao == 0:
            t_exec = time.time() - self._instante_inicio_execucao
        else:
            t_exec = (self._instante_fim_execucao -
                      self._instante_inicio_execucao)
        return t_exec

    @property
    def numero_tentativas(self) -> int:
        Caso._verifica_caso_configurado(self._numero_tentativas)
        return self._numero_tentativas

    @property
    def numero_processadores(self) -> int:
        Caso._verifica_caso_configurado(self._numero_processadores)
        return self._numero_processadores

    @property
    def sucesso(self) -> bool:
        Caso._verifica_caso_configurado(self._sucesso)
        return self._sucesso


class CasoNEWAVE(Caso):

    def __init__(self) -> None:
        super().__init__()
        self._ano: int = None  # type: ignore
        self._mes: int = None  # type: ignore

    def configura_dados_caso(self,
                             ano: int,
                             mes: int):
        self._ano = ano
        self._mes = mes

    def _obtem_numero_processadores(self) -> int:
        CasoNEWAVE._verifica_caso_configurado(self._configuracoes)
        minimo = self.configuracoes.processadores_minimos_newave
        maximo = self.configuracoes.processadores_maximos_newave
        ajuste = self.configuracoes.ajuste_processadores_newave
        num_proc = minimo
        if ajuste:
            num_proc = maximo
        return num_proc

    @property
    def ano(self) -> int:
        CasoNEWAVE._verifica_caso_configurado(self._ano)
        return self._ano

    @property
    def mes(self) -> int:
        CasoNEWAVE._verifica_caso_configurado(self._mes)
        return self._mes
