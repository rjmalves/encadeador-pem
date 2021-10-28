from logging import Logger
import pandas as pd  # type: ignore
import numpy as np  # type: ignore
from typing import Dict, List
from os.path import join

from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.modelos.dadoscaso import NOME_ARQUIVO_ESTADO
from encadeador.modelos.arvorecasos import ArvoreCasos
from encadeador.controladores.sintetizadorcaso import DIRETORIO_RESUMO_CASO


class DadosEstudo:
    """
    Resumo dos dados de execução e agrupamento de casos do
    estudo encadeado.
    """
    def __init__(self,
                 resumo_estados: pd.DataFrame,
                 resumo_newaves: pd.DataFrame,
                 resumo_decomps: pd.DataFrame) -> None:
        self._resumo_estados = resumo_estados
        self._resumo_newaves = resumo_newaves
        self._resumo_decomps = resumo_decomps

    @staticmethod
    def resume_arvore(arvore: ArvoreCasos,
                      log: Logger) -> "DadosEstudo":

        def le_resumo_newave(resumo_estados: pd.DataFrame,
                             resumo_newaves: pd.DataFrame,
                             caso: CasoNEWAVE,
                             primeiro: bool):
            arq_resumo = join(caso.caminho, NOME_ARQUIVO_ESTADO)
            log.info(f"Arquivo de resumo: {arq_resumo}")
            df_resumo = pd.read_csv(arq_resumo, index_col=0)
            log.info(f"DF de resumo: {df_resumo}")
            if resumo_estados.empty:
                log.info("Vazio")
                resumo_estados = df_resumo
            else:
                resumo_estados = pd.concat([resumo_estados, df_resumo],
                                           ignore_index=True)
            log.info(f"DF de resumo dos estados: {resumo_estados}")
            return resumo_estados, resumo_newaves

        def le_resumo_decomp(resumo_estados: pd.DataFrame,
                             resumo_decomps: pd.DataFrame,
                             caso: CasoDECOMP,
                             primeiro: bool):
            # Faz o resumo do estado dos casos
            arq_resumo = join(caso.caminho, NOME_ARQUIVO_ESTADO)
            log.info(f"Arquivo de resumo: {arq_resumo}")
            df_resumo = pd.read_csv(arq_resumo, index_col=0)
            log.info(f"DF de resumo: {df_resumo}")
            if resumo_estados.empty:
                log.info("Vazio")
                resumo_estados = df_resumo
            else:
                resumo_estados = pd.concat([resumo_estados, df_resumo],
                                           ignore_index=True)
            # Faz o resumo das variáveis
            diretorio_resumo = join(caso.caminho, DIRETORIO_RESUMO_CASO)
            log.info(f"Diretorio de resumo: {diretorio_resumo}")
            subsistemas = ["SE", "S", "NE", "N"]
            nome = f"{caso.ano}_{str(caso.mes).zfill(2)}_rv{caso.revisao}"
            cmo = pd.read_csv(join(diretorio_resumo,
                                   "cmo.csv"),
                              index_col=0)
            earm_subsis = pd.read_csv(join(diretorio_resumo,
                                           "earm_subsis.csv"),
                                      index_col=0)
            earm_sin = pd.read_csv(join(diretorio_resumo,
                                        "earm_sin.csv"),
                                   index_col=0)
            gt_subsis = pd.read_csv(join(diretorio_resumo,
                                         "gt_subsis.csv"),
                                    index_col=0)
            gt_sin = pd.read_csv(join(diretorio_resumo,
                                      "gt_sin.csv"),
                                 index_col=0)
            gh_subsis = pd.read_csv(join(diretorio_resumo,
                                         "gh_subsis.csv"),
                                    index_col=0)
            gh_sin = pd.read_csv(join(diretorio_resumo,
                                      "gh_sin.csv"),
                                 index_col=0)
            merc_subsis = pd.read_csv(join(diretorio_resumo,
                                           "mercado_subsis.csv"),
                                      index_col=0)
            merc_sin = pd.read_csv(join(diretorio_resumo,
                                        "mercado_sin.csv"),
                                   index_col=0)
            nomes: List[str] = []
            cmos: Dict[str, List[float]] = {s: [] for s in subsistemas}
            earms_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
            gts_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
            ghs_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
            mercs_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
            earms_sin: List[float] = []
            gts_sin: List[float] = []
            ghs_sin: List[float] = []
            mercs_sin: List[float] = []
            if primeiro:
                nomes.append("Inicial")
                for s in subsistemas:
                    cmos[s].append(np.nan)
                    earms_sub[s].append(np.nan)
                    gts_sub[s].append(np.nan)
                    ghs_sub[s].append(np.nan)
                    mercs_sub[s].append(np.nan)
                earms_sin.append(float(earm_sin["Inicial"]))
                gts_sin.append(np.nan)
                ghs_sin.append(np.nan)
                mercs_sin.append(np.nan)
            nomes.append(nome)
            for s in subsistemas:
                cmos[s].append(float(cmo.loc[(cmo["Subsistema"] == s) &
                                             (cmo["Patamar"] == "Médio"),
                                             "Estágio 1"]))
                eas = float(earm_subsis.loc[earm_subsis["Subsistema"] == s,
                                            "Estágio 1"])
                earms_sub[s].append(eas)
                gts = float(gt_subsis.loc[gt_subsis["Subsistema"] == s,
                                          "Estágio 1"])
                gts_sub[s].append(gts)
                ghs = float(gh_subsis.loc[gh_subsis["Subsistema"] == s,
                                          "Estágio 1"])
                ghs_sub[s].append(ghs)
                mrs = float(merc_subsis.loc[merc_subsis["Subsistema"] == s,
                                            "Estágio 1"])
                mercs_sub[s].append(mrs)
            earms_sin.append(float(earm_sin["Estágio 1"]))
            gts_sin.append(float(gt_sin["Estágio 1"]))
            ghs_sin.append(float(gh_sin["Estágio 1"]))
            mercs_sin.append(float(merc_sin["Estágio 1"]))
            # Organiza os dados em um DataFrame
            dados_variaveis: Dict[str, list] = {c: [] for c in colunas_resumo}
            dados_variaveis["Caso"] = nomes
            for s in subsistemas:
                dados_variaveis[f"CMO {s}"] = cmos[s]
                dados_variaveis[f"EARM {s}"] = earms_sub[s]
                dados_variaveis[f"GT {s}"] = gts_sub[s]
                dados_variaveis[f"GH {s}"] = ghs_sub[s]
                dados_variaveis[f"Mercado {s}"] = mercs_sub[s]
            dados_variaveis["EARM SIN"] = earms_sin
            dados_variaveis["GT SIN"] = gts_sin
            dados_variaveis["GH SIN"] = ghs_sin
            dados_variaveis["Mercado SIN"] = mercs_sin
            df_variaveis = pd.DataFrame(data=dados_variaveis)
            log.info(f"DF de variáveis: {dados_variaveis}")
            if resumo_decomps.empty:
                resumo_decomps = df_variaveis
            else:
                resumo_decomps = pd.concat([resumo_decomps, df_variaveis],
                                           ignore_index=True)
            log.info(f"DF de resumo das variáveis: {resumo_decomps}")
            return resumo_estados, resumo_decomps

        def le_resumo(caso: Caso,
                      resumo_estados: pd.DataFrame,
                      resumo_newaves: pd.DataFrame,
                      resumo_decomps: pd.DataFrame,
                      primeiro: bool):
            if isinstance(caso, CasoNEWAVE):
                log.info("Caso de NEWAVE")
                e, n = le_resumo_newave(resumo_estados,
                                        resumo_newaves,
                                        caso,
                                        primeiro)
                resumo_estados = e
                resumo_newaves = n
            elif isinstance(caso, CasoDECOMP):
                log.info("Caso de DECOMP")
                e, d = le_resumo_decomp(resumo_estados,
                                        resumo_decomps,
                                        caso,
                                        primeiro)
                resumo_estados = e
                resumo_decomps = d
            else:
                raise ValueError(f"Caso do tipo {type(caso)} não suportado" +
                                 "na síntese do estudo")
            return resumo_estados, resumo_newaves, resumo_decomps

        colunas_estado = ["Caminho",
                          "Nome",
                          "Ano",
                          "Mes",
                          "Revisao",
                          "Estado",
                          "Tentativas",
                          "Processadores",
                          "Sucesso",
                          "Entrada Fila",
                          "Inicio Execucao",
                          "Fim Execucao",
                          "Programa"]

        colunas_resumo = ["Caso",
                          "CMO SE",
                          "CMO S",
                          "CMO NE",
                          "CMO N",
                          "EARM SIN",
                          "EARM SE",
                          "EARM S",
                          "EARM NE",
                          "EARM N",
                          "GT SIN",
                          "GT SE",
                          "GT S",
                          "GT NE",
                          "GT N",
                          "GH SIN",
                          "GH SE",
                          "GH S",
                          "GH NE",
                          "GH N",
                          "Mercado SIN",
                          "Mercado SE",
                          "Mercado S",
                          "Mercado NE",
                          "Mercado N",
                          ]

        resumo_estados = pd.DataFrame(columns=colunas_estado)
        resumo_newaves = pd.DataFrame()
        resumo_decomps = pd.DataFrame(columns=colunas_resumo)
        for i, c in enumerate(arvore.casos):
            if c.sucesso:
                # Passa i == 1 para significar o primeiro DECOMP (segundo caso)
                log.info(f"Lendo resumo do caso {c.nome}")
                e, n, d = le_resumo(c,
                                    resumo_estados,
                                    resumo_newaves,
                                    resumo_decomps,
                                    i == 1)
                resumo_estados = e
                resumo_newaves = n
                resumo_decomps = d
        log.info("Resumos finais:")
        log.info(resumo_estados)
        log.info(resumo_newaves)
        log.info(resumo_decomps)
        return DadosEstudo(resumo_estados, resumo_newaves, resumo_decomps)

    @property
    def resumo_estados(self) -> pd.DataFrame:
        return self._resumo_estados

    @property
    def resumo_newaves(self) -> pd.DataFrame:
        return self._resumo_newaves

    @property
    def resumo_decomps(self) -> pd.DataFrame:
        return self._resumo_decomps
