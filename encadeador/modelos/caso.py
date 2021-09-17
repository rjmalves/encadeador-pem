from abc import abstractmethod
import time

# STUB
class Configuracoes:
    def __init__(self) -> None:
        self.diretorio_newave = "newave"
        self.versao_newave = "v27.4.12"
        self.diretorio_instalacao_newaves = "~/versoes/NEWAVE"
        self.processadores_minimos_newave = 0
        self.processadores_maximos_newave = 0
        self.ajuste_processadores_newave = False


class Caso:
    """
    Classe base que representa todos os casos executandos no
    estudo encadeado. É responsável por armazenar informações
    associadas ao caso. Não lida com a sua execução, pré ou pós
    processamento dos decks.
    """
    def __init__(self,) -> None:
        self._caminho_caso = None
        self._nome_caso = None
        self._configuracoes = None
        self._instante_entrada_fila = None
        self._instante_inicio_execucao = None
        self._instante_fim_execucao = None
        self._numero_tentativas = None
        self._sucesso = None

    @abstractmethod
    def configura_caso(self,
                       caminho: str,
                       nome: str,
                       cfg: Configuracoes):
        pass

    def reseta_paramatros_execucao(self):
        self._instante_entrada_fila = 0
        self._instante_inicio_execucao = 0
        self._instante_fim_execucao = 0
        self._sucesso = False

    def inicializa_paramatros_execucao(self):
        self.reseta_paramatros_execucao()
        self._numero_tentativas = 0
        self._sucesso = False

    def coloca_caso_na_fila(self):
        self.reseta_paramatros_execucao()
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
        if self._instante_inicio_execucao == 0:
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
            t_exec = 0
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
    def sucesso(self) -> bool:
        Caso._verifica_caso_configurado(self._sucesso)
        return self._sucesso


class CasoNEWAVE(Caso):
    def __init__(self) -> None:
        super().__init__()