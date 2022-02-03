from os import chdir
from logging import Logger
from typing import Optional

from encadeador.modelos.caso import Caso
from encadeador.modelos.arvorecasos import ArvoreCasos
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.executorcaso import ExecutorCaso
from encadeador.controladores.sintetizadorestudo import SintetizadorEstudo


class App:
    def __init__(self, cfg: Configuracoes, log: Logger) -> None:
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

    def executa(self) -> bool:
        def verifica_ordenacao_casos(
            proximo: Caso,
            ultimo_newave: Optional[Caso],
            ultimo_decomp: Optional[Caso],
        ):
            indice_prox = self._arvore.indice_caso(proximo)
            indice_ult_nw = self._arvore.indice_caso(ultimo_newave)
            indice_ult_dc = self._arvore.indice_caso(ultimo_decomp)
            if any([indice_prox < indice_ult_nw, indice_prox < indice_ult_dc]):
                self._log.error(
                    "Erro de execução: o próximo caso é"
                    + f"anterior aos últimos: {indice_prox}"
                    + f" < ({indice_ult_nw}, {indice_ult_dc})"
                )
                raise RuntimeError()

        self._log.info(f"Iniciando Encadeador - {self._cfg.nome_estudo}")
        sucesso = True
        try:
            self.__constroi_arvore_casos()
            sintetizador = SintetizadorEstudo(self._arvore, self._log)
            if not sintetizador.sintetiza_estudo():
                raise RuntimeError()
            # Refaz a síntese como estão as coisas
            while not self._arvore.terminou:
                chdir(self._cfg.caminho_base_estudo)
                prox = self._arvore.proximo_caso
                if prox is None:
                    raise RuntimeError()
                executor = ExecutorCaso.factory(prox, self._log)
                ult_nw = self._arvore.ultimo_newave
                ult_dc = self._arvore.ultimo_decomp
                # Verificação de segurança: o próximo caso deve ser
                # posterior aos últimos NW e DC
                verifica_ordenacao_casos(prox, ult_nw, ult_dc)
                # Salva o caso a ser executado na raiz do estudo
                # para visualização
                SintetizadorEstudo.sintetiza_proximo_caso(prox, self._cfg)
                executor.executa_e_monitora_caso(
                    self._arvore.casos_concluidos, ult_nw
                )
                # Faz a síntese do estudo
                if not sintetizador.sintetiza_estudo():
                    raise RuntimeError()
            self._log.info("Finalizando Encadeador")
            return sucesso
        except Exception as e:
            self._log.error(f"Execução do Encadeador interrompida: {e}")
            if prox is not None:
                prox.finaliza_caso(False, erro=True)
                executor._armazenador.armazena_caso()
            sucesso = False
            raise e
