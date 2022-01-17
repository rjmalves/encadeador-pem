from abc import abstractmethod
from os import chdir
from typing import Optional, List

from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.controladores.flexibilizadorcaso import Flexibilizador
from encadeador.controladores.monitorcaso import MonitorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorCaso
from encadeador.controladores.sintetizadorcaso import SintetizadorNEWAVE
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.utils.log import Log


class ExecutorCaso:

    def __init__(self,
                 caso: Caso) -> None:
        self._caso = caso
        self._monitor = MonitorCaso.factory(caso)
        self._sintetizador = SintetizadorCaso.factory(caso)

    @staticmethod
    def factory(caso: Caso) -> 'ExecutorCaso':
        if isinstance(caso, CasoNEWAVE):
            return ExecutorNEWAVE(caso)
        elif isinstance(caso, CasoDECOMP):
            return ExecutorDECOMP(caso)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    @abstractmethod
    def executa_e_monitora_caso(self,
                                casos_anteriores: List[Caso],
                                ultimo_newave: Optional[CasoNEWAVE]):
        pass


class ExecutorNEWAVE(ExecutorCaso):

    def __init__(self,
                 caso: CasoNEWAVE) -> None:
        super().__init__(caso)

    def executa_e_monitora_caso(self,
                                casos_anteriores: List[Caso],
                                ultimo_newave: Optional[CasoNEWAVE]):

        chdir(self._caso.caminho)

        # Se não é o primeiro NEWAVE, apaga os cortes do último
        if ultimo_newave is not None:
            sint_ultimo = SintetizadorNEWAVE(ultimo_newave)
            if sint_ultimo.verifica_cortes_extraidos():
                sint_ultimo.deleta_cortes()

        if not self._monitor.inicializa(casos_anteriores):
            Log.log().error(f"Erro na inicialização do {self._caso.nome}")
            raise RuntimeError()

        if not self._monitor.submete():
            Log.log().error(f"Erro na submissão do {self._caso.nome}")
            raise RuntimeError()

        # TODO - TRANSFERIR ESSE PEDAÇO DA LÓGICA PARA A "RAIZ" DO ESTUDO
        # E IMPLEMENTAR MAIS UMA CAMADA DE OBSERVER/STATE PATTERN
        # Loop para esperar o caso terminar

        # Se o NEWAVE não obteve sucesso, está com erro
        if not self._caso.estado != EstadoCaso.CONCLUIDO:
            Log.log().error(f"Erro nas saídas do {self._caso.nome}")
            raise RuntimeError()

        if not self._sintetizador.sintetiza_caso():
            Log.log().error(f"Erro ao sintetizar caso: {self._caso.nome}")
            raise RuntimeError()


class ExecutorDECOMP(ExecutorCaso):

    def __init__(self,
                 caso: CasoDECOMP) -> None:
        super().__init__(caso)

    def executa_e_monitora_caso(self,
                                casos_anteriores: List[Caso],
                                ultimo_newave: Optional[CasoNEWAVE]):

        chdir(self._caso.caminho)

        if not self._preparador.prepara_caso(caso_cortes=ultimo_newave):
            Log.log().error(f"Erro na preparação do DC {self._caso.nome}")
            raise RuntimeError()
        if not self._preparador.encadeia_variaveis(casos_anteriores):
            Log.log().error(f"Erro no encadeamento do DC {self._caso.nome}")
            raise RuntimeError()

        # Primeira execução: obrigatória
        if not self._monitor.executa_caso():
            Log.log().error(f"Erro na execução do DC {self._caso.nome}")
            raise RuntimeError()
        if not self._sintetizador.sintetiza_caso():
            Log.log().error(f"Erro ao sintetizar caso: {self._caso.nome}")
            raise RuntimeError()
        if not self._armazenador.armazena_caso():
            Log.log().error(f"Erro ao armazenar caso: {self._caso.nome}")
            raise RuntimeError()

        while not self._caso.sucesso:
            max_flex = Configuracoes().maximo_flexibilizacoes_revisao
            if self._caso.numero_flexibilizacoes >= max_flex:
                Log.log().error("Máximo de flexibilizações atingido" +
                                f" no DC {self._caso.nome}")
                raise RuntimeError()
            # Se ainda pode flexibilizar
            flexibilizador = Flexibilizador.factory(self._caso)
            if not flexibilizador.flexibiliza():
                Log.log().error("Erro na flexibilização do caso " +
                                f"{self._caso.nome}")
                raise RuntimeError()
            if not self._monitor.executa_caso():
                Log.log().error(f"Erro na execução do DC {self._caso.nome}")
                raise RuntimeError()
            if not self._sintetizador.sintetiza_caso():
                Log.log().error(f"Erro ao sintetizar caso: {self._caso.nome}")
                raise RuntimeError()
            if not self._armazenador.armazena_caso():
                Log.log().error(f"Erro ao armazenar caso: {self._caso.nome}")
                raise RuntimeError()
