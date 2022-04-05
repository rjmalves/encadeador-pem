from os.path import join
from typing import Optional
import pandas as pd  # type: ignore

from encadeador.modelos.caso import Caso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.dadoscaso import NOME_ARQUIVO_ESTADO
from encadeador.modelos.estudo import Estudo
from encadeador.utils.log import Log
from encadeador.utils.io import escreve_df_em_csv, escreve_arquivo_json

ARQUIVO_PROXIMO_CASO = "proximo_caso.json"
ARQUIVO_RESUMO_NEWAVES = "newaves_encadeados.csv"
ARQUIVO_RESUMO_DECOMPS = "decomps_encadeados.csv"
ARQUIVO_RESUMO_RESERVATORIOS = "reservatorios_encadeados.csv"
ARQUIVO_RESUMO_DEFLUENCIAS = "defluencias_encadeadas.csv"
ARQUIVO_CONVERGENCIA_NEWAVES = "convergencia_newaves.csv"
ARQUIVO_CONVERGENCIA_DECOMPS = "convergencia_decomps.csv"
ARQUIVO_INVIABILIDADES_DECOMPS = "inviabilidades_decomps.csv"


class SintetizadorEstudo:
    def __init__(self, estudo: Estudo) -> None:
        self._estudo = estudo

    @staticmethod
    def sintetiza_proximo_caso(caso: Optional[Caso]):
        df_proximo_caso = pd.DataFrame(columns=["Caminho"])
        if caso is not None:
            caminho = join(caso.caminho, NOME_ARQUIVO_ESTADO)
            df_proximo_caso["Caminho"] = [caminho]
            arq = join(
                Configuracoes().caminho_base_estudo, ARQUIVO_PROXIMO_CASO
            )
            escreve_arquivo_json(arq, {"Caminho": caminho})
        else:
            raise RuntimeError(
                "Erro na sintese do próximo caso do estudo " + "encadeado."
            )

    def sintetiza_estudo(self) -> bool:
        Log.log().info("Sintetizando dados do estudo encadeado")
        dados = self._estudo.dados
        diretorio_estudo = Configuracoes().caminho_base_estudo
        resumo_newaves = join(diretorio_estudo, ARQUIVO_RESUMO_NEWAVES)
        resumo_decomps = join(diretorio_estudo, ARQUIVO_RESUMO_DECOMPS)
        resumo_reservatorios = join(
            diretorio_estudo, ARQUIVO_RESUMO_RESERVATORIOS
        )
        resumo_defluencias = join(
            diretorio_estudo, ARQUIVO_RESUMO_DEFLUENCIAS
        )
        convergencias_newaves = join(
            diretorio_estudo, ARQUIVO_CONVERGENCIA_NEWAVES
        )
        convergencias_decomps = join(
            diretorio_estudo, ARQUIVO_CONVERGENCIA_DECOMPS
        )
        inviabilidades_decomps = join(
            diretorio_estudo, ARQUIVO_INVIABILIDADES_DECOMPS
        )
        escreve_df_em_csv(dados.resumo_newaves, resumo_newaves)
        escreve_df_em_csv(dados.resumo_decomps, resumo_decomps)
        escreve_df_em_csv(dados.resumo_reservatorios, resumo_reservatorios)
        escreve_df_em_csv(dados.resumo_defluencias, resumo_defluencias)
        escreve_df_em_csv(dados.convergencias_newaves, convergencias_newaves)
        escreve_df_em_csv(dados.convergencias_decomps, convergencias_decomps)
        escreve_df_em_csv(dados.inviabilidades_decomps, inviabilidades_decomps)
        return True
