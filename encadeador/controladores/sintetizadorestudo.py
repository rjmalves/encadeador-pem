from os.path import join
from logging import Logger
from typing import Optional
import pandas as pd  # type: ignore

from encadeador.modelos.caso import Caso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.dadoscaso import NOME_ARQUIVO_ESTADO
from encadeador.modelos.arvorecasos import ArvoreCasos
from encadeador.modelos.dadosestudo import DadosEstudo

ARQUIVO_PROXIMO_CASO = "proximo_caso.csv"
ARQUIVO_RESUMO_ESTADOS = "estudo_encadeado.csv"
ARQUIVO_RESUMO_NEWAVES = "newaves_encadeados.csv"
ARQUIVO_RESUMO_DECOMPS = "decomps_encadeados.csv"


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
        df_proximo_caso.to_csv(join(cfg.caminho_base_estudo,
                                    ARQUIVO_PROXIMO_CASO))

    def sintetiza_estudo(self) -> bool:
        try:
            self._log.info("Sintetizando dados do estudo encadeado")
            dados = DadosEstudo.resume_arvore(self._arvore)
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
            dados.resumo_estados.to_csv(resumo_estados)
            dados.resumo_newaves.to_csv(resumo_newaves)
            dados.resumo_decomps.to_csv(resumo_decomps)
            self._log.info("Dados sintetizados com sucesso")
            return True
        except Exception as e:
            self._log.info(f"Erro na síntese do estudo encadeado: {e}")
            return False
