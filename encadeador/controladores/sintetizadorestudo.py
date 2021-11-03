from os.path import join
from logging import Logger
from typing import Optional
import time
import pandas as pd  # type: ignore

from encadeador.modelos.caso import Caso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.dadoscaso import NOME_ARQUIVO_ESTADO
from encadeador.modelos.arvorecasos import ArvoreCasos
from encadeador.modelos.dadosestudo import DadosEstudo
from encadeador.modelos.dadoscaso import INTERVALO_RETRY_ESCRITA
from encadeador.modelos.dadoscaso import MAX_RETRY_ESCRITA

ARQUIVO_PROXIMO_CASO = "proximo_caso.csv"
ARQUIVO_RESUMO_ESTADOS = "estudo_encadeado.csv"
ARQUIVO_RESUMO_NEWAVES = "newaves_encadeados.csv"
ARQUIVO_RESUMO_DECOMPS = "decomps_encadeados.csv"
ARQUIVO_CONVERGENCIA_NEWAVES = "convergencia_newaves.csv"
ARQUIVO_CONVERGENCIA_DECOMPS = "convergencia_decomps.csv"
ARQUIVO_INVIABILIDADES_DECOMPS = "inviabilidades_decomps.csv"


class SintetizadorEstudo:

    def __init__(self,
                 arvore: ArvoreCasos,
                 log: Logger) -> None:
        self._arvore = arvore
        self._log = log

    @staticmethod
    def sintetiza_proximo_caso(caso: Optional[Caso],
                               cfg: Configuracoes):
        # TODO - quando usar o padrão Singleton, não precisa
        # mais passar as Configurações
        df_proximo_caso = pd.DataFrame(columns=["Caminho"])
        if caso is not None:
            caminho = join(caso.caminho, NOME_ARQUIVO_ESTADO)
            df_proximo_caso["Caminho"] = [caminho]
        num_retry = 0
        while num_retry < MAX_RETRY_ESCRITA:
            try:
                df_proximo_caso.to_csv(join(cfg.caminho_base_estudo,
                                            ARQUIVO_PROXIMO_CASO))
                return
            except OSError:
                num_retry += 1
                time.sleep(INTERVALO_RETRY_ESCRITA)
                continue
            except BlockingIOError:
                num_retry += 1
                time.sleep(INTERVALO_RETRY_ESCRITA)
                continue
        raise RuntimeError("Erro na sintese do próximo caso do estudo " +
                           "encadeado.")

    def sintetiza_estudo(self) -> bool:
        self._log.info("Sintetizando dados do estudo encadeado")
        num_retry = 0
        while num_retry < MAX_RETRY_ESCRITA:
            try:
                dados = DadosEstudo.resume_arvore(self._arvore, self._log)
                # TODO - Ao invés de pegar o primeiro caso para ter as
                # configurações, substituir pelo padrão Singleton
                cfg = self._arvore.casos[0].configuracoes
                diretorio_estudo = cfg.caminho_base_estudo
                resumo_estados = join(diretorio_estudo,
                                      ARQUIVO_RESUMO_ESTADOS)
                resumo_newaves = join(diretorio_estudo,
                                      ARQUIVO_RESUMO_NEWAVES)
                resumo_decomps = join(diretorio_estudo,
                                      ARQUIVO_RESUMO_DECOMPS)
                convergencias_newaves = join(diretorio_estudo,
                                             ARQUIVO_CONVERGENCIA_NEWAVES)
                convergencias_decomps = join(diretorio_estudo,
                                             ARQUIVO_CONVERGENCIA_DECOMPS)
                inviabilidades_decomps = join(diretorio_estudo,
                                              ARQUIVO_INVIABILIDADES_DECOMPS)
                dados.resumo_estados.to_csv(resumo_estados)
                dados.resumo_newaves.to_csv(resumo_newaves)
                dados.resumo_decomps.to_csv(resumo_decomps)
                dados.convergencias_newaves.to_csv(convergencias_newaves)
                dados.convergencias_decomps.to_csv(convergencias_decomps)
                dados.inviabilidades_decomps.to_csv(inviabilidades_decomps)
                return True
            except OSError:
                num_retry += 1
                time.sleep(INTERVALO_RETRY_ESCRITA)
                continue
            except BlockingIOError:
                num_retry += 1
                time.sleep(INTERVALO_RETRY_ESCRITA)
                continue
        self._log.info("Erro na síntese do estudo encadeado")
        return False
