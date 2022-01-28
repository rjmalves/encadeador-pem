from os.path import join, isdir, isfile
from os import makedirs, listdir, remove
from abc import abstractmethod
from typing import List
from zipfile import ZipFile
import numpy as np  # type: ignore
import pandas as pd  # type: ignore
from encadeador.modelos.estadocaso import EstadoCaso  # type: ignore

from inewave.newave import Arquivos, PMO  # type: ignore
from idecomp.decomp.relato import Relato
from idecomp.decomp.relgnl import RelGNL
from idecomp.decomp. inviabunic import InviabUnic
from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.utils.processadordecomp import ProcessadorDecomp
from encadeador.utils.log import Log
from encadeador.utils.io import escreve_df_em_csv, le_df_de_csv

DIRETORIO_RESUMO_CASO = "resumo"
PADRAO_ZIP_DECK_NEWAVE = "deck_"
PADRAO_ZIP_SAIDAS_NEWAVE = "saidas_"
PADRAO_ZIP_SAIDAS_NWLISTOP = "_out"


class SintetizadorCaso:

    def __init__(self,
                 caso: Caso) -> None:
        self._caso = caso
        # Cria o diretório de resumos se não existir
        self.__cria_diretorio_resumos()

    @staticmethod
    def factory(caso: Caso) -> 'SintetizadorCaso':
        if isinstance(caso, CasoNEWAVE):
            return SintetizadorNEWAVE(caso)
        elif isinstance(caso, CasoDECOMP):
            return SintetizadorDECOMP(caso)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    def __cria_diretorio_resumos(self):
        diretorio = join(self.caso.caminho,
                         DIRETORIO_RESUMO_CASO)
        if not isdir(diretorio):
            makedirs(diretorio)

    @abstractmethod
    def sintetiza_caso(self) -> bool:
        pass

    @property
    def caso(self) -> Caso:
        return self._caso


class SintetizadorNEWAVE(SintetizadorCaso):

    def __init__(self,
                 caso: CasoNEWAVE):
        super().__init__(caso)

    def sintetiza_caso(self) -> bool:
        Log.log().info("Sintetizando informações do" +
                       f" caso {self._caso.nome}")
        pmo = PMO.le_arquivo(self.caso.caminho)
        caminho_saida = join(self.caso.caminho,
                             DIRETORIO_RESUMO_CASO)
        # Convergência do pmo.dat
        escreve_df_em_csv(pmo.convergencia,
                          join(caminho_saida,
                               "convergencia.csv"))
        # Custos do pmo.dat
        escreve_df_em_csv(pmo.custo_operacao_series_simuladas,
                          join(caminho_saida,
                               "custos.csv"))
        # CMO, EARM, GT, GH do NWLISTOP
        # TODO
        return True

    def __procura_zip_saida(self) -> str:
        caminho = self.caso.caminho
        arqs_dir = listdir(caminho)
        padrao = PADRAO_ZIP_SAIDAS_NEWAVE
        try:
            arq_zip = [a for a in arqs_dir if padrao in a][0]
            if not isfile(join(caminho, arq_zip)):
                raise ValueError
            Log.log().info(f"Encontrado arquivo com {padrao} em {caminho}")
            return join(caminho, arq_zip)
        except Exception as e:
            Log.log().error(f"Nao foi encontrado um zip com {padrao}")
            raise e

    @property
    def _nomes_arquivos_cortes(self) -> List[str]:
        arquivos = Arquivos.le_arquivo(self.caso.caminho)
        return [arquivos.cortes, arquivos.cortesh]

    def extrai_cortes(self):
        arq_zip = self.__procura_zip_saida()
        try:
            with ZipFile(arq_zip, "r") as obj_zip:
                arqs = obj_zip.namelist()
                for a in self._nomes_arquivos_cortes:
                    if a not in arqs:
                        raise ValueError(a)
                    Log.log().info(f"Extraindo {a} de {arq_zip}...")
                    obj_zip.extract(a, self.caso.caminho)
        except ValueError as v:
            Log.log().error(f"Não foi encontrado o arquivo {str(v)}")

    def deleta_cortes(self):
        for a in self._nomes_arquivos_cortes:
            caminho = join(self.caso.caminho, a)
            if isfile(caminho):
                remove(caminho)
                Log.log().info(f"Arquivo {caminho} deletado")
            else:
                raise ValueError(f"Arquivo {caminho} não deletado")

    def verifica_cortes_extraidos(self) -> bool:
        arqs_dir = listdir(self.caso.caminho)
        cortes_extraidos = [c in arqs_dir for c in
                            self._nomes_arquivos_cortes]
        return all(cortes_extraidos)


class SintetizadorDECOMP(SintetizadorCaso):

    def __init__(self,
                 caso: CasoDECOMP):
        super().__init__(caso)

    @staticmethod
    def __processa_earm_sin(earm_subsis: pd.DataFrame,
                            earmax: pd.DataFrame) -> pd.DataFrame:
        valores_earmax = earmax["Earmax"].to_numpy()
        earmax_sin = np.sum(valores_earmax)
        earms_absolutos = earm_subsis.copy()
        cols_estagios = ["Inicial"] + [c for c in list(earms_absolutos.columns)
                                       if "Estágio" in c]
        for c in cols_estagios:
            earms_absolutos[c] *= (valores_earmax / 100)
        earms_sin = earms_absolutos.loc[:, cols_estagios].sum(axis=0)
        earms_sin /= earmax_sin
        earms_sin *= 100
        df = pd.DataFrame(columns=cols_estagios)
        df.loc[0, :] = earms_sin
        return df

    @staticmethod
    def __processa_dado_sin(dado_subsis: pd.DataFrame) -> pd.DataFrame:
        cols_estagios = [c for c in list(dado_subsis.columns)
                         if "Estágio" in c]
        dado_sin = dado_subsis.loc[:, cols_estagios].sum(axis=0)
        df = pd.DataFrame(columns=cols_estagios)
        df.loc[0, :] = dado_sin
        return df

    @staticmethod
    def __processa_gh(balanco: pd.DataFrame) -> pd.DataFrame:
        gh = balanco.loc[:, ["Estágio", "Subsistema", "Ghid",
                             "Itaipu50", "Itaipu60"]].copy()
        gh["Ghid"] = gh["Ghid"] + gh["Itaipu60"]
        gh = gh.drop(columns=["Itaipu50", "Itaipu60"])
        # Formata da mesma maneira das demais tabelas do relato
        estagios = list(set(gh["Estágio"].tolist()))
        subsistemas = list(set(gh["Subsistema"].tolist()))
        cols_df = [f"Estágio {e}" for e in estagios]
        df = pd.DataFrame(columns=["Subsistema"] + cols_df)
        for i, s in enumerate(subsistemas):
            valores_sub = [float(gh.loc[(gh["Estágio"] == e) &
                                        (gh["Subsistema"] == s),
                                        "Ghid"])
                           for e in estagios]
            df.loc[i, "Subsistema"] = s
            df.loc[i, cols_df] = valores_sub
        return df

    @staticmethod
    def __processa_mercado(balanco: pd.DataFrame) -> pd.DataFrame:
        merc = balanco.loc[:, ["Estágio", "Subsistema", "Mercado"]]
        # Formata da mesma maneira das demais tabelas do relato
        estagios = list(set(merc["Estágio"].tolist()))
        subsistemas = list(set(merc["Subsistema"].tolist()))
        cols_df = [f"Estágio {e}" for e in estagios]
        df = pd.DataFrame(columns=["Subsistema"] + cols_df)
        for i, s in enumerate(subsistemas):
            valores_sub = [float(merc.loc[(merc["Estágio"] == e) &
                                          (merc["Subsistema"] == s),
                                          "Mercado"])
                           for e in estagios]
            df.loc[i, "Subsistema"] = s
            df.loc[i, cols_df] = valores_sub
        return df

    @staticmethod
    def __processa_deficit(balanco: pd.DataFrame) -> pd.DataFrame:
        merc = balanco.loc[:, ["Estágio", "Subsistema", "Deficit"]]
        # Formata da mesma maneira das demais tabelas do relato
        estagios = list(set(merc["Estágio"].tolist()))
        subsistemas = list(set(merc["Subsistema"].tolist()))
        cols_df = [f"Estágio {e}" for e in estagios]
        df = pd.DataFrame(columns=["Subsistema"] + cols_df)
        for i, s in enumerate(subsistemas):
            valores_sub = [float(merc.loc[(merc["Estágio"] == e) &
                                          (merc["Subsistema"] == s),
                                          "Deficit"])
                           for e in estagios]
            df.loc[i, "Subsistema"] = s
            df.loc[i, cols_df] = valores_sub
        return df

    @staticmethod
    def __processa_gt_percentual_max(relato: Relato,
                                     relgnl: RelGNL) -> pd.DataFrame:
        return ProcessadorDecomp.gt_percentual_maxima(relato, relgnl)

    @staticmethod
    def __processa_gt_percentual_flex(relato: Relato,
                                      relgnl: RelGNL) -> pd.DataFrame:
        return ProcessadorDecomp.gt_percentual_flexivel(relato, relgnl)

    def sintetiza_caso(self) -> bool:
        Log.log().info("Sintetizando informações do" +
                       f" caso {self._caso.nome}")
        arq_relato = f"relato.rv{self._caso.revisao}"
        arq_relgnl = f"relgnl.rv{self._caso.revisao}"
        caminho_saida = join(self.caso.caminho,
                             DIRETORIO_RESUMO_CASO)
        relato = Relato.le_arquivo(self.caso.caminho, arq_relato)
        relgnl = RelGNL.le_arquivo(self.caso.caminho, arq_relgnl)
        # Convergência do relato.rvX
        conv = relato.convergencia
        cols_conv = list(conv.columns)
        conv["Flexibilizacao"] = self.caso.numero_flexibilizacoes
        conv = conv[["Flexibilizacao"] + cols_conv]
        if isfile(join(caminho_saida, "convergencia.csv")):
            conv_anterior = le_df_de_csv(join(caminho_saida,
                                              "convergencia.csv"))
            conv = pd.concat([conv_anterior, conv],
                             ignore_index=True)
        # Inviabilidades e Déficit do inviab_unic.rvX
        arq_inviab = f"inviab_unic.rv{self._caso.revisao}"
        inviab_unic = InviabUnic.le_arquivo(self.caso.caminho,
                                            arq_inviab)
        inviab = inviab_unic.inviabilidades_simulacao_final
        cols_inviab = list(inviab.columns)
        inviab["Flexibilizacao"] = self.caso.numero_flexibilizacoes
        inviab = inviab[["Flexibilizacao"] + cols_inviab]
        if isfile(join(caminho_saida, "inviabilidades.csv")):
            inviab_anterior = le_df_de_csv(join(caminho_saida,
                                                "inviabilidades.csv"))
            inviab = pd.concat([inviab_anterior, inviab],
                               ignore_index=True)
        # Escreve em disco
        conv.to_csv(join(caminho_saida, "convergencia.csv"),
                    header=True,
                    encoding="utf-8")
        inviab.to_csv(join(caminho_saida, "inviabilidades.csv"),
                      header=True,
                      encoding="utf-8")
        # Se não teve sucesso no caso, só exporta esses dados
        if self.caso.estado != EstadoCaso.CONCLUIDO:
            return True
        # CMO, EARM, GT, GH do relato.rvX
        cmo = relato.cmo_medio_subsistema
        earm_subsis = relato.energia_armazenada_subsistema
        earmax = relato.energia_armazenada_maxima_subsistema
        earm_sin = SintetizadorDECOMP.__processa_earm_sin(earm_subsis,
                                                          earmax)
        gt_subsis = relato.geracao_termica_subsistema
        gt_sin = SintetizadorDECOMP.__processa_dado_sin(gt_subsis)
        gt_perc_m = SintetizadorDECOMP.__processa_gt_percentual_max(relato,
                                                                    relgnl)
        gt_perc_f = SintetizadorDECOMP.__processa_gt_percentual_flex(relato,
                                                                     relgnl)
        balanco = relato.balanco_energetico
        gh_subsis = SintetizadorDECOMP.__processa_gh(balanco)
        gh_sin = SintetizadorDECOMP.__processa_dado_sin(gh_subsis)
        merc_subsis = SintetizadorDECOMP.__processa_mercado(balanco)
        def_subsis = SintetizadorDECOMP.__processa_deficit(balanco)
        merc_sin = SintetizadorDECOMP.__processa_dado_sin(merc_subsis)
        def_sin = SintetizadorDECOMP.__processa_dado_sin(def_subsis)
        # Exporta os dados
        escreve_df_em_csv(cmo,
                          join(caminho_saida, "cmo.csv"))
        escreve_df_em_csv(earm_subsis,
                          join(caminho_saida, "earm_subsis.csv"))
        escreve_df_em_csv(earm_sin,
                          join(caminho_saida, "earm_sin.csv"))
        escreve_df_em_csv(gt_subsis,
                          join(caminho_saida, "gt_subsis.csv"))
        escreve_df_em_csv(gt_sin,
                          join(caminho_saida, "gt_sin.csv"))
        escreve_df_em_csv(gt_perc_m,
                          join(caminho_saida, "gt_percentual_max.csv"))
        escreve_df_em_csv(gt_perc_f,
                          join(caminho_saida, "gt_percentual_flex.csv"))
        escreve_df_em_csv(gh_subsis,
                          join(caminho_saida, "gh_subsis.csv"))
        escreve_df_em_csv(gh_sin,
                          join(caminho_saida, "gh_sin.csv"))
        escreve_df_em_csv(merc_subsis,
                          join(caminho_saida, "mercado_subsis.csv"))
        escreve_df_em_csv(def_subsis,
                          join(caminho_saida, "deficit_subsis.csv"))
        escreve_df_em_csv(merc_sin,
                          join(caminho_saida, "mercado_sin.csv"))
        escreve_df_em_csv(def_sin,
                          join(caminho_saida, "deficit_sin.csv"))
        return True
