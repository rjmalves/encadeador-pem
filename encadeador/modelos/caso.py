from abc import abstractmethod
import time

# STUB
class Configuracoes:
    def __init__(self) -> None:
        self.nome_estudo = "Backtest CPAMP 2015-2021 CVAR AxL"
        self.diretorio_newave = "newave"
        self.versao_newave = "newave"
        self.diretorio_instalacao_newaves = "./tests/_arquivos"
        self.processadores_minimos_newave = 72
        self.processadores_maximos_newave = 72
        self.ajuste_processadores_newave = False
        self.diretorio_decomp = "newave"
        self.versao_decomp = "newave"
        self.diretorio_instalacao_decomps = "./tests/_arquivos"
        self.processadores_minimos_decomp = 72
        self.processadores_maximos_decomp = 72
        self.ajuste_processadores_decomp = False
        self.gerenciador_fila = "SGE"
        self.adequa_decks_newave = True
        self.cvar = (50, 35)
        self.parpa = 3
        self.adequa_decks_decomp = True
        self.maximo_iteracoes_decomp = 500
        self.fator_aumento_gap_decomp = 10
        self.gap_maximo_decomp = 1e-1
        self.arquivo_lista_casos = "lista_casos.txt"


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
        self._ano_caso: int = None  # type: ignore
        self._mes_caso: int = None  # type: ignore
        self._revisao_caso: int = None  # type: ignore
        self._configuracoes: Configuracoes = None  # type: ignore
        self._instante_entrada_fila: float = None  # type: ignore
        self._instante_inicio_execucao: float = None  # type: ignore
        self._instante_fim_execucao: float = None  # type: ignore
        self._numero_tentativas: int = None  # type: ignore
        self._numero_processadores: int = None  # type: ignore
        self._sucesso: bool = None  # type: ignore

    def configura_caso(self,
                       caminho: str,
                       ano: int,
                       mes: int,
                       revisao: int,
                       cfg: Configuracoes):
        self._configuracoes = cfg
        self._caminho_caso = caminho
        self._ano_caso = ano
        self._mes_caso = mes
        self._revisao_caso = revisao
        self._nome_caso = self._constroi_nome_caso()

    @abstractmethod
    def _constroi_nome_caso(self) -> str:
        pass

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
    def instante_entrada_fila(self) -> float:
        return self._instante_entrada_fila

    @property
    def instante_inicio_execucao(self) -> float:
        return self._instante_inicio_execucao

    @property
    def instante_fim_execucao(self) -> float:
        return self._instante_fim_execucao

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
    def ano(self) -> int:
        Caso._verifica_caso_configurado(self._ano_caso)
        return self._ano_caso

    @property
    def mes(self) -> int:
        Caso._verifica_caso_configurado(self._mes_caso)
        return self._mes_caso

    @property
    def revisao(self) -> int:
        Caso._verifica_caso_configurado(self._revisao_caso)
        return self._revisao_caso

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

    def _obtem_numero_processadores(self) -> int:
        # TODO - Ler o dger.dat e conferir as restrições de número
        # de processadores (séries forward)
        CasoNEWAVE._verifica_caso_configurado(self._configuracoes)
        minimo = self.configuracoes.processadores_minimos_newave
        maximo = self.configuracoes.processadores_maximos_newave
        ajuste = self.configuracoes.ajuste_processadores_newave
        num_proc = minimo
        if ajuste:
            num_proc = maximo
        return num_proc

    def _constroi_nome_caso(self) -> str:
        return f"{self.configuracoes.nome_estudo} - NW {self.mes}/{self.ano}"


class CasoDECOMP(Caso):

    def __init__(self) -> None:
        super().__init__()

    def _obtem_numero_processadores(self) -> int:
        # TODO - Ler o dadger.rvX e conferir as restrições de número
        # de processadores (séries do 2º mês)
        CasoDECOMP._verifica_caso_configurado(self._configuracoes)
        minimo = self.configuracoes.processadores_minimos_decomp
        maximo = self.configuracoes.processadores_maximos_decomp
        ajuste = self.configuracoes.ajuste_processadores_decomp
        num_proc = minimo
        if ajuste:
            num_proc = maximo
        return num_proc
