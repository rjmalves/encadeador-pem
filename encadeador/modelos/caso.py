from abc import abstractmethod
import time

from encadeador.modelos.dadoscaso import DadosCaso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.estadojob import EstadoJob


class Caso:
    """
    Classe base que representa todos os casos executados no
    estudo encadeado. É responsável por armazenar informações
    associadas ao caso. Não lida com a sua execução, pré ou pós
    processamento dos decks.
    """
    def __init__(self,) -> None:
        self._dados: DadosCaso = None  # type: ignore
        self._configuracoes: Configuracoes = None  # type: ignore

    @staticmethod
    def factory(prog: str) -> 'Caso':
        if prog == "NEWAVE":
            return CasoNEWAVE()
        elif prog == "DECOMP":
            return CasoDECOMP()
        else:
            raise ValueError(f"Programa {prog} não suportado")

    @abstractmethod
    def configura_caso(self,
                       caminho: str,
                       ano: int,
                       mes: int,
                       revisao: int,
                       cfg: Configuracoes):
        pass

    @abstractmethod
    def _constroi_nome_caso(self,
                            ano: int,
                            mes: int,
                            revisao: int) -> str:
        pass

    @abstractmethod
    def _obtem_numero_processadores(self) -> int:
        pass

    def reseta_parametros_execucao(self):
        self._dados.instante_entrada_fila = 0
        self._dados.instante_inicio_execucao = 0
        self._dados.instante_fim_execucao = 0
        self._dados.sucesso = False

    def inicializa_parametros_execucao(self):
        self.reseta_parametros_execucao()
        self._dados.numero_tentativas = 0
        procs = self._obtem_numero_processadores()
        self._dados.numero_processadores = procs
        self._dados.sucesso = False
        self._dados.estado = EstadoJob.NAO_INICIADO

    def coloca_caso_na_fila(self):
        self.reseta_parametros_execucao()
        self._dados.instante_entrada_fila = time.time()
        self._dados.estado = EstadoJob.ESPERANDO

    def inicia_caso(self):
        self._dados.instante_inicio_execucao = time.time()
        self._dados.numero_tentativas += 1
        self._dados.estado = EstadoJob.EXECUTANDO

    def finaliza_caso(self,
                      sucesso: bool,
                      erro: bool = False):
        self._dados.sucesso = sucesso
        self._dados.instante_fim_execucao = time.time()
        if erro:
            self._dados.estado = EstadoJob.ERRO
        else:
            self._dados.estado = EstadoJob.CONCLUIDO

    def adiciona_flexibilizacao(self):
        self._dados.adiciona_flexibilizacao()

    def recupera_caso_dos_dados(self,
                                dados: DadosCaso,
                                cfg: Configuracoes):
        self._dados = dados
        self._configuracoes = cfg

    @staticmethod
    def _verifica_caso_configurado(valor):
        if valor is None:
            raise ValueError("Caso não configurado!")
        return valor

    @property
    def caminho(self) -> str:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.caminho

    @property
    def nome(self) -> str:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.nome

    @property
    def configuracoes(self) -> Configuracoes:
        Caso._verifica_caso_configurado(self._configuracoes)
        return self._configuracoes

    @property
    def instante_entrada_fila(self) -> float:
        return self._dados.instante_entrada_fila

    @property
    def instante_inicio_execucao(self) -> float:
        return self._dados.instante_inicio_execucao

    @property
    def instante_fim_execucao(self) -> float:
        return self._dados.instante_fim_execucao

    @property
    def tempo_fila(self) -> float:
        Caso._verifica_caso_configurado(self._dados)
        if self.instante_entrada_fila == 0:
            t_fila = 0.
        elif self.instante_inicio_execucao == 0:
            t_fila = time.time() - self.instante_entrada_fila
        else:
            t_fila = (self.instante_inicio_execucao -
                      self.instante_entrada_fila)
        return t_fila

    @property
    def tempo_execucao(self) -> float:
        Caso._verifica_caso_configurado(self._dados)
        if self.instante_inicio_execucao == 0:
            t_exec = 0.
        elif self.instante_fim_execucao == 0:
            t_exec = time.time() - self.instante_inicio_execucao
        else:
            t_exec = (self.instante_fim_execucao -
                      self.instante_inicio_execucao)
        return t_exec

    @property
    def ano(self) -> int:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.ano

    @property
    def mes(self) -> int:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.mes

    @property
    def revisao(self) -> int:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.revisao

    @property
    def numero_tentativas(self) -> int:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.numero_tentativas

    @property
    def numero_processadores(self) -> int:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.numero_processadores

    @property
    def sucesso(self) -> bool:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.sucesso

    @property
    def numero_flexibilizacoes(self) -> int:
        Caso._verifica_caso_configurado(self._dados)
        return self._dados.numero_flexibilizacoes

    @property
    def estado(self) -> EstadoJob:
        return self._dados.estado


class CasoNEWAVE(Caso):

    def __init__(self) -> None:
        super().__init__()

    # Override
    def configura_caso(self,
                       caminho: str,
                       ano: int,
                       mes: int,
                       revisao: int,
                       cfg: Configuracoes):
        self._configuracoes = cfg
        nome = self._constroi_nome_caso(ano, mes, revisao)
        procs = self._obtem_numero_processadores()
        self._dados = DadosCaso.obtem_dados_do_caso("NEWAVE",
                                                    caminho,
                                                    nome,
                                                    ano,
                                                    mes,
                                                    revisao,
                                                    procs)

    def _obtem_numero_processadores(self) -> int:
        # TODO - Ler o dger.dat e conferir as restrições de número
        # de processadores (séries forward)
        minimo = self.configuracoes.processadores_minimos_newave
        maximo = self.configuracoes.processadores_maximos_newave
        ajuste = self.configuracoes.ajuste_processadores_newave
        num_proc = minimo
        if ajuste:
            num_proc = maximo
        return num_proc

    def _constroi_nome_caso(self,
                            ano: int,
                            mes: int,
                            revisao: int) -> str:
        return f"{self.configuracoes.nome_estudo} - NW {mes}/{ano}"


class CasoDECOMP(Caso):

    def __init__(self) -> None:
        super().__init__()

    def configura_caso(self,
                       caminho: str,
                       ano: int,
                       mes: int,
                       revisao: int,
                       cfg: Configuracoes):
        self._configuracoes = cfg
        nome = self._constroi_nome_caso(ano, mes, revisao)
        procs = self._obtem_numero_processadores()
        self._dados = DadosCaso.obtem_dados_do_caso("DECOMP",
                                                    caminho,
                                                    nome,
                                                    ano,
                                                    mes,
                                                    revisao,
                                                    procs)

    def _obtem_numero_processadores(self) -> int:
        # TODO - Ler o dadger.rvX e conferir as restrições de número
        # de processadores (séries do 2º mês)
        minimo = self.configuracoes.processadores_minimos_decomp
        maximo = self.configuracoes.processadores_maximos_decomp
        ajuste = self.configuracoes.ajuste_processadores_decomp
        num_proc = minimo
        if ajuste:
            num_proc = maximo
        return num_proc

    def _constroi_nome_caso(self,
                            ano: int,
                            mes: int,
                            revisao: int) -> str:
        return (f"{self.configuracoes.nome_estudo} - DC" +
                f" {mes}/{ano} rv{revisao}")
