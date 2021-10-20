from os import chdir
from logging import Logger

from encadeador.modelos.arvorecasos import ArvoreCasos
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.executorcaso import ExecutorCaso


class App:

    def __init__(self,
                 cfg: Configuracoes,
                 log: Logger) -> None:
        self._cfg = cfg
        self._log = log
        self._arvore: ArvoreCasos = None  # type: ignore

    def __constroi_arvore_casos(self) -> ArvoreCasos:
        self._arvore = ArvoreCasos(self._cfg, self._log)
        self._arvore.le_arquivo_casos()
        if not self._arvore.constroi_casos():
            self._log.error("Erro na construção dos casos")
            raise RuntimeError()
        return self._arvore

    def executa(self):
        self._log.info(f"Iniciando Encadeador - {self._cfg.nome_estudo}")
        try:
            self.__constroi_arvore_casos()
            while not self._arvore.terminou:
                chdir(self._cfg.caminho_base_estudo)
                prox = self._arvore.proximo_caso
                executor = ExecutorCaso.factory(prox, self._log)
                ult_nw = self._arvore.ultimo_newave
                ult_dc = self._arvore.ultimo_decomp
                executor.executa_e_monitora_caso(ult_nw, ult_dc)
        except Exception:
            self._log.error("Execução do Encadeador interrompida")
        finally:
            self._log.info("Finalizando Encadeador")
