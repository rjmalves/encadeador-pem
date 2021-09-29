from os import chdir
from logging import Logger

from encadeador.modelos.arvorecasos import ArvoreCasos
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.controladores.preparadorcaso import PreparadorCasoNEWAVE
from encadeador.controladores.preparadorcaso import PreparadorCasoDECOMP
from encadeador.controladores.monitorcaso import MonitorNEWAVE
from encadeador.controladores.monitorcaso import MonitorDECOMP
from encadeador.controladores.sintetizadorcaso import SintetizadorCasoNEWAVE


class App:

    def __init__(self,
                 cfg: Configuracoes,
                 log: Logger,
                 dir_base: str) -> None:
        self._cfg = cfg
        self._log = log
        self._dir_base = dir_base

    def __constroi_arvore_casos(self) -> ArvoreCasos:
        self._arvore = ArvoreCasos(self._cfg, self._log, self._dir_base)
        self._arvore.le_arquivo_casos()
        if not self._arvore.constroi_casos():
            raise RuntimeError("Erro na construção dos Casos")
        return self._arvore

    def __executa_newave(self, caso: CasoNEWAVE):
        # Se não é o primeiro NEWAVE, apaga os cortes do último
        if self._arvore.ultimo_newave is not None:
            sintetizador = SintetizadorCasoNEWAVE(self._arvore.ultimo_newave,
                                                  self._log)
            if sintetizador.verifica_cortes_extraidos():
                sintetizador.deleta_cortes()

        preparador = PreparadorCasoNEWAVE(caso, self._log)
        monitor = MonitorNEWAVE(caso, self._log)
        if not preparador.prepara_caso():
            raise RuntimeError(f"Erro na preparação do NW {caso.nome}")
        if not preparador.encadeia_variaveis(self._arvore.ultimo_decomp):
            raise RuntimeError(f"Erro no encadeamento do NW {caso.nome}")
        if not monitor.executa_caso():
            raise RuntimeError(f"Erro na execução do NW {caso.nome}")

    def __executa_decomp(self, caso: CasoDECOMP):
        preparador = PreparadorCasoDECOMP(caso, self._log)
        monitor = MonitorDECOMP(caso, self._log)
        if not preparador.prepara_caso(caso_cortes=self._arvore.ultimo_newave):
            raise RuntimeError(f"Erro na preparação do DCP {caso.nome}")
        if not preparador.encadeia_variaveis(self._arvore.ultimo_decomp):
            raise RuntimeError(f"Erro no encadeamento do DCP {caso.nome}")
        if not monitor.executa_caso():
            raise RuntimeError(f"Erro na execução do DCP {caso.nome}")

    def __atualiza_informacoes_caso(self, caso: Caso):
        n_anterior = self._arvore._dados_casos[caso.nome]
        self._arvore._dados_casos[caso.nome] = n_anterior + 1

    def executa(self):
        self._log.info(f"Iniciando Encadeador - {self._cfg.nome_estudo}")
        self.__constroi_arvore_casos()
        while not self._arvore.terminou:
            chdir(self._dir_base)
            prox = self._arvore.proximo_caso
            if (self._arvore._dados_casos[prox.nome] >=
                    self._cfg._maximo_flexibilizacoes_revisao):
                raise RuntimeError("Máximo de flexibilizações" +
                                   f" no caso: {prox.nome}")
            chdir(prox.caminho)
            if isinstance(prox, CasoNEWAVE):
                self.__executa_newave(prox)
            elif isinstance(prox, CasoDECOMP):
                self.__executa_decomp(prox)
            self.__atualiza_informacoes_caso(prox)
        self._log.info("Finalizando Encadeador")
