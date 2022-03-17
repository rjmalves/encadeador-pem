import pandas as pd  # type: ignore
import numpy as np  # type: ignore
from typing import Dict, List, Any, Tuple
from os.path import join

from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.modelos.dadoscaso import NOME_ARQUIVO_ESTADO
from encadeador.controladores.sintetizadorcaso import DIRETORIO_RESUMO_CASO
from encadeador.modelos.estadocaso import EstadoCaso


class DadosEstudo:
    """
    Resumo dos dados de execução e agrupamento de casos do
    estudo encadeado.
    """

    COLUNAS_ESTADO = [
        "Caminho",
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
        "Programa",
    ]

    COLUNAS_CUSTOS = [
        "Caso",
        "GERACAO TERMICA",
        "DEFICIT",
        "VERTIMENTO",
        "EXCESSO ENERGIA",
        "VIOLACAO CAR",
        "VIOLACAO SAR",
        "VIOL. OUTROS USOS",
        "VIOLACAO VZMIN",
        "INTERCAMBIO",
        "VERT. FIO N. TURB.",
        "VIOLACAO GHMIN",
    ]

    COLUNAS_RESUMO = [
        "Caso",
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

    COLUNAS_CONVERGENCIA = [
        "Caso",
        "Flexibilizacao",
        "Iteração",
        "Zinf",
        "Zsup",
        "Gap (%)",
        "Tempo (s)",
    ]

    COLUNAS_INVIABS = [
        "Caso",
        "Flexibilizacao",
        "Estagio",
        "Restricao",
        "Violacao",
        "Unidade",
    ]

    def __init__(
        self,
        nome: str,
        caminho: str,
        instante_inicio_execucao: float,
        diretorios_revisoes: List[str],
        diretorios_casos: List[str],
        nomes_casos: List[str],
        tempos_fila_casos: List[float],
        tempos_execucao_casos: List[float],
        estados_casos: List[EstadoCaso],
    ):
        self._nome = nome
        self._caminho = caminho
        self._instante_inicio_execucao = instante_inicio_execucao
        self._diretorios_revisoes = diretorios_revisoes
        self._diretorios_casos = diretorios_casos
        self._nomes_casos = nomes_casos
        self._tempos_fila_casos = tempos_fila_casos
        self._tempos_execucao_casos = tempos_execucao_casos
        self._estados_casos = estados_casos
        # DataFrames com dados do estudo
        self._resumo_estados = pd.DataFrame()
        self._resumo_newaves = pd.DataFrame()
        self._resumo_decomps = pd.DataFrame()
        self._resumo_reservatorios = pd.DataFrame()
        self._convergencias_newaves = pd.DataFrame()
        self._convergencias_decomps = pd.DataFrame()
        self._inviabilidades_decomps = pd.DataFrame()

    @staticmethod
    def from_json(json_dict: Dict[str, Any]) -> "DadosEstudo":
        return DadosEstudo(
            json_dict["_nome"],
            json_dict["_caminho"],
            json_dict["_instante_inicio_execucao"],
            json_dict["_diretorios_revisoes"],
            json_dict["_diretorios_casos"],
            json_dict["_nomes_casos"],
            json_dict["_tempos_fila_casos"],
            json_dict["_tempos_execucao_casos"],
            [EstadoCaso.factory(e) for e in json_dict["_estados_casos"]],
        )

    def to_json(self) -> Dict[str, Any]:
        return {
            "_nome": self._nome,
            "_caminho": str(self._caminho),
            "_instante_inicio_execucao": self._instante_inicio_execucao,
            "_diretorios_revisoes": self._diretorios_revisoes,
            "_diretorios_casos": self._diretorios_casos,
            "_nomes_casos": self._nomes_casos,
            "_tempos_fila_casos": self._tempos_fila_casos,
            "_tempos_execucao_casos": self._tempos_execucao_casos,
            "_estados_casos": [str(e.value) for e in self._estados_casos],
        }

    def resume_casos(self, casos: List[Caso]):
        self._nomes_casos = [c.nome for c in casos]
        self._tempos_fila_casos = [c.tempo_fila for c in casos]
        self._tempos_execucao_casos = [c.tempo_execucao for c in casos]
        self._estados_casos = [c.estado for c in casos]

    @staticmethod
    def __le_resumo_newave(
        resumo_estados: pd.DataFrame,
        resumo_newaves: pd.DataFrame,
        caso: CasoNEWAVE,
        primeiro: bool,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        arq_resumo = join(caso.caminho, NOME_ARQUIVO_ESTADO)
        df_resumo = pd.read_csv(arq_resumo, index_col=0)
        if resumo_estados.empty:
            resumo_estados = df_resumo
        else:
            resumo_estados = pd.concat(
                [resumo_estados, df_resumo], ignore_index=True
            )
        # Faz o resumo dos custos
        diretorio_resumo = join(caso.caminho, DIRETORIO_RESUMO_CASO)
        custos = pd.read_csv(join(diretorio_resumo, "custos.csv"), index_col=0)
        dados_variaveis: Dict[str, list] = {
            c: [] for c in DadosEstudo.COLUNAS_CUSTOS
        }
        nome = f"{caso.ano}_{str(caso.mes).zfill(2)}_rv{caso.revisao}"
        dados_variaveis["Caso"] = [nome]
        for c in DadosEstudo.COLUNAS_CUSTOS:
            if c == "Caso":
                continue
            custo = float(custos.loc[c.ljust(18), "Valor Esperado"])
            dados_variaveis[c].append(custo)
        df_variaveis = pd.DataFrame(data=dados_variaveis)
        if resumo_newaves.empty:
            resumo_newaves = df_variaveis
        else:
            resumo_newaves = pd.concat(
                [resumo_newaves, df_variaveis], ignore_index=True
            )
        return resumo_estados, resumo_newaves

    @staticmethod
    def __le_resumo_decomp(
        resumo_estados: pd.DataFrame,
        resumo_decomps: pd.DataFrame,
        resumo_reservatorios: pd.DataFrame,
        caso: CasoDECOMP,
        primeiro: bool,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        # Faz o resumo do estado dos casos
        arq_resumo = join(caso.caminho, NOME_ARQUIVO_ESTADO)
        df_resumo = pd.read_csv(arq_resumo, index_col=0)
        if resumo_estados.empty:
            resumo_estados = df_resumo
        else:
            resumo_estados = pd.concat(
                [resumo_estados, df_resumo], ignore_index=True
            )
        # Faz o resumo das variáveis
        diretorio_resumo = join(caso.caminho, DIRETORIO_RESUMO_CASO)
        subsistemas = ["SE", "S", "NE", "N"]
        nome = f"{caso.ano}_{str(caso.mes).zfill(2)}_rv{caso.revisao}"
        cmo = pd.read_csv(join(diretorio_resumo, "cmo.csv"), index_col=0)
        earm_reserv = pd.read_csv(
            join(diretorio_resumo, "earm_reserv.csv"), index_col=0
        )
        earm_subsis = pd.read_csv(
            join(diretorio_resumo, "earm_subsis.csv"), index_col=0
        )
        earm_sin = pd.read_csv(
            join(diretorio_resumo, "earm_sin.csv"), index_col=0
        )
        gt_subsis = pd.read_csv(
            join(diretorio_resumo, "gt_subsis.csv"), index_col=0
        )
        gt_sin = pd.read_csv(join(diretorio_resumo, "gt_sin.csv"), index_col=0)
        gt_perc_m = pd.read_csv(
            join(diretorio_resumo, "gt_percentual_max.csv"), index_col=0
        )
        gt_perc_f = pd.read_csv(
            join(diretorio_resumo, "gt_percentual_flex.csv"), index_col=0
        )
        gh_subsis = pd.read_csv(
            join(diretorio_resumo, "gh_subsis.csv"), index_col=0
        )
        gh_sin = pd.read_csv(join(diretorio_resumo, "gh_sin.csv"), index_col=0)
        merc_subsis = pd.read_csv(
            join(diretorio_resumo, "mercado_subsis.csv"), index_col=0
        )
        merc_sin = pd.read_csv(
            join(diretorio_resumo, "mercado_sin.csv"), index_col=0
        )
        def_subsis = pd.read_csv(
            join(diretorio_resumo, "deficit_subsis.csv"), index_col=0
        )
        nomes: List[str] = []
        cmos: Dict[str, List[float]] = {s: [] for s in subsistemas}
        earms_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
        gts_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
        gts_perc_m_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
        gts_perc_f_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
        ghs_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
        mercs_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
        defs_sub: Dict[str, List[float]] = {s: [] for s in subsistemas}
        earms_sin: List[float] = []
        gts_sin: List[float] = []
        gts_perc_m_sin: List[float] = []
        gts_perc_f_sin: List[float] = []
        ghs_sin: List[float] = []
        mercs_sin: List[float] = []
        defs_sin: List[float] = []
        if primeiro:
            nomes.append("Inicial")
            for s in subsistemas:
                cmos[s].append(np.nan)
                eas = earm_subsis.loc[earm_subsis["Subsistema"] == s, "Inicial"]
                earms_sub[s].append(float(eas))
                gts_sub[s].append(np.nan)
                gts_perc_m_sub[s].append(np.nan)
                gts_perc_f_sub[s].append(np.nan)
                ghs_sub[s].append(np.nan)
                mercs_sub[s].append(np.nan)
                defs_sub[s].append(np.nan)
            earms_sin.append(float(earm_sin["Inicial"]))
            gts_sin.append(np.nan)
            gts_perc_m_sin.append(np.nan)
            gts_perc_f_sin.append(np.nan)
            ghs_sin.append(np.nan)
            mercs_sin.append(np.nan)
            defs_sin.append(np.nan)
        nomes.append(nome)
        for s in subsistemas:
            cmos[s].append(
                float(
                    cmo.loc[
                        (cmo["Subsistema"] == s) & (cmo["Patamar"] == "Médio"),
                        "Estágio 1",
                    ]
                )
            )
            eas = float(
                earm_subsis.loc[earm_subsis["Subsistema"] == s, "Estágio 1"]
            )
            earms_sub[s].append(eas)
            gts = float(
                gt_subsis.loc[gt_subsis["Subsistema"] == s, "Estágio 1"]
            )
            gts_sub[s].append(gts)
            gts_p_m = float(
                gt_perc_m.loc[gt_perc_m["Subsistema"] == s, "Estágio 1"]
            )
            gts_perc_m_sub[s].append(gts_p_m)
            gts_p_f = float(
                gt_perc_f.loc[gt_perc_f["Subsistema"] == s, "Estágio 1"]
            )
            gts_perc_f_sub[s].append(gts_p_f)
            ghs = float(
                gh_subsis.loc[gh_subsis["Subsistema"] == s, "Estágio 1"]
            )
            ghs_sub[s].append(ghs)
            mrs = float(
                merc_subsis.loc[merc_subsis["Subsistema"] == s, "Estágio 1"]
            )
            mercs_sub[s].append(mrs)
            cols = [c for c in list(def_subsis.columns) if "Estágio" in c]
            dfs = float(
                def_subsis.loc[def_subsis["Subsistema"] == s, cols].sum(axis=1)
            )
            defs_sub[s].append(dfs)

        earms_sin.append(float(earm_sin["Estágio 1"]))
        gts_sin.append(float(gt_sin["Estágio 1"]))
        gts_p_m = float(
            gt_perc_m.loc[gt_perc_m["Subsistema"] == "SIN", "Estágio 1"]
        )
        gts_perc_m_sin.append(gts_p_m)
        gts_p_f = float(
            gt_perc_f.loc[gt_perc_f["Subsistema"] == "SIN", "Estágio 1"]
        )
        gts_perc_f_sin.append(gts_p_f)
        ghs_sin.append(float(gh_sin["Estágio 1"]))
        mercs_sin.append(float(merc_sin["Estágio 1"]))
        defs_sin.append(sum([defs_sub[s][0] for s in subsistemas]))
        # Organiza os dados em um DataFrame
        dados_variaveis: Dict[str, list] = {
            c: [] for c in DadosEstudo.COLUNAS_RESUMO
        }
        dados_variaveis["Caso"] = nomes
        for s in subsistemas:
            dados_variaveis[f"CMO {s}"] = cmos[s]
            dados_variaveis[f"EARM {s}"] = earms_sub[s]
            dados_variaveis[f"GT {s}"] = gts_sub[s]
            dados_variaveis[f"GT {s} (% MAX)"] = gts_perc_m_sub[s]
            dados_variaveis[f"GT {s} (% FLEX)"] = gts_perc_f_sub[s]
            dados_variaveis[f"GH {s}"] = ghs_sub[s]
            dados_variaveis[f"Mercado {s}"] = mercs_sub[s]
            dados_variaveis[f"Déficit {s}"] = defs_sub[s]
        dados_variaveis["EARM SIN"] = earms_sin
        dados_variaveis["GT SIN"] = gts_sin
        dados_variaveis["GT SIN (% MAX)"] = gts_perc_m_sin
        dados_variaveis["GT SIN (% FLEX)"] = gts_perc_f_sin
        dados_variaveis["GH SIN"] = ghs_sin
        dados_variaveis["Mercado SIN"] = mercs_sin
        dados_variaveis["Déficit SIN"] = defs_sin
        df_variaveis = pd.DataFrame(data=dados_variaveis)
        if resumo_decomps.empty:
            resumo_decomps = df_variaveis
        else:
            resumo_decomps = pd.concat(
                [resumo_decomps, df_variaveis], ignore_index=True
            )

        # Organiza os dados dos reservatórios
        reservatorios: pd.DataFrame = earm_reserv.transpose()
        reservatorios.columns = reservatorios.loc["Usina"]
        reservatorios = reservatorios.drop(index="Usina")
        cols = list(reservatorios.columns)
        reservatorios["Estagio"] = reservatorios.index
        reservatorios["Caso"] = nome
        reservatorios = reservatorios[["Caso", "Estagio"] + cols]
        if resumo_reservatorios.empty:
            resumo_reservatorios = reservatorios.reset_index(drop=True)
        else:
            resumo_reservatorios = pd.concat(
                [resumo_reservatorios, reservatorios], ignore_index=True
            )

        return resumo_estados, resumo_decomps, resumo_reservatorios

    @staticmethod
    def __le_resumo(
        caso: Caso,
        resumo_estados: pd.DataFrame,
        resumo_newaves: pd.DataFrame,
        resumo_decomps: pd.DataFrame,
        resumo_reservatorios: pd.DataFrame,
        primeiro: bool,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:

        if isinstance(caso, CasoNEWAVE):
            e, n = DadosEstudo.__le_resumo_newave(
                resumo_estados, resumo_newaves, caso, primeiro
            )
            resumo_estados = e
            resumo_newaves = n
        elif isinstance(caso, CasoDECOMP):
            e, d, r = DadosEstudo.__le_resumo_decomp(
                resumo_estados,
                resumo_decomps,
                resumo_reservatorios,
                caso,
                primeiro,
            )
            resumo_estados = e
            resumo_decomps = d
            resumo_reservatorios = r
        else:
            raise ValueError(
                f"Caso do tipo {type(caso)} não suportado"
                + "na síntese do estudo"
            )
        return (
            resumo_estados,
            resumo_newaves,
            resumo_decomps,
            resumo_reservatorios,
        )

    @staticmethod
    def __le_convergencia(
        caso: Caso,
        convergencia_newaves: pd.DataFrame,
        convergencia_decomps: pd.DataFrame,
    ):
        def le_convergencia_newaves(
            convergencia_newaves: pd.DataFrame, caso: CasoNEWAVE
        ):
            diretorio_resumo = join(caso.caminho, DIRETORIO_RESUMO_CASO)
            conv = pd.read_csv(
                join(diretorio_resumo, "convergencia.csv"), index_col=0
            )
            dados_conv: Dict[str, list] = {
                c: [] for c in DadosEstudo.COLUNAS_CONVERGENCIA
            }

            iteracoes = conv["Iteração"][2::3].to_numpy()
            zinfs = conv["ZINF"][2::3].to_numpy()
            zsups = conv["ZSUP Iteração"][2::3].to_numpy()
            gaps = 100 * (zsups[1:] - zinfs[1:]) / zinfs[:-1]
            tempos = conv["Tempo (s)"][2::3]
            nome = f"{caso.ano}_{str(caso.mes).zfill(2)}_rv{caso.revisao}"
            dados_conv["Caso"] = [nome] * len(iteracoes)
            dados_conv["Flexibilizacao"] = [0] * len(iteracoes)
            dados_conv["Iteração"] = list(iteracoes)
            dados_conv["Zinf"] = list(zinfs)
            dados_conv["Zsup"] = list(zsups)
            dados_conv["Gap (%)"] = [0] + list(gaps)
            dados_conv["Tempo (s)"] = list(tempos)
            df_conv = pd.DataFrame(data=dados_conv)
            if convergencia_newaves.empty:
                convergencia_newaves = df_conv
            else:
                convergencia_newaves = pd.concat(
                    [convergencia_newaves, df_conv], ignore_index=True
                )
            return convergencia_newaves

        def le_convergencia_decomps(
            convergencia_decomps: pd.DataFrame, caso: CasoDECOMP
        ):
            diretorio_resumo = join(caso.caminho, DIRETORIO_RESUMO_CASO)
            conv = pd.read_csv(
                join(diretorio_resumo, "convergencia.csv"), index_col=0
            )
            dados_conv: Dict[str, list] = {
                c: [] for c in DadosEstudo.COLUNAS_CONVERGENCIA
            }

            iteracoes = conv["Iteração"].tolist()
            zinfs = conv["Zinf"].tolist()
            zsups = conv["Zsup"].tolist()
            gaps = conv["Gap (%)"].tolist()
            tempos = conv["Tempo (s)"].tolist()
            flexibilizacoes = conv["Flexibilizacao"].tolist()
            nome = f"{caso.ano}_{str(caso.mes).zfill(2)}_rv{caso.revisao}"
            dados_conv["Caso"] = [nome] * len(iteracoes)
            dados_conv["Flexibilizacao"] = flexibilizacoes
            dados_conv["Iteração"] = iteracoes
            dados_conv["Zinf"] = zinfs
            dados_conv["Zsup"] = zsups
            dados_conv["Gap (%)"] = gaps
            dados_conv["Tempo (s)"] = tempos
            df_conv = pd.DataFrame(data=dados_conv)
            if convergencia_decomps.empty:
                convergencia_decomps = df_conv
            else:
                convergencia_decomps = pd.concat(
                    [convergencia_decomps, df_conv], ignore_index=True
                )
            return convergencia_decomps

        if isinstance(caso, CasoNEWAVE):
            n = le_convergencia_newaves(convergencia_newaves, caso)
            convergencia_newaves = n
        elif isinstance(caso, CasoDECOMP):
            d = le_convergencia_decomps(convergencia_decomps, caso)
            convergencia_decomps = d
        else:
            raise ValueError(
                f"Caso do tipo {type(caso)} não suportado"
                + "na síntese do estudo"
            )
        return convergencia_newaves, convergencia_decomps

    @staticmethod
    def __le_inviabilidades(
        caso: Caso,
        inviabilidades_newaves: pd.DataFrame,
        inviabilidades_decomps: pd.DataFrame,
    ):
        def le_inviabilidades_decomps(
            inviabilidades_decomps: pd.DataFrame, caso: CasoDECOMP
        ):
            diretorio_resumo = join(caso.caminho, DIRETORIO_RESUMO_CASO)
            inviab = pd.read_csv(
                join(diretorio_resumo, "inviabilidades.csv"), index_col=0
            )
            dados_inviab: Dict[str, list] = {
                c: [] for c in DadosEstudo.COLUNAS_INVIABS
            }

            flexibilizacoes = inviab["Flexibilizacao"].tolist()
            estagios = inviab["Estágio"].tolist()
            cenarios = inviab["Cenário"].tolist()
            restricoes = inviab["Restrição"].tolist()
            violacoes = inviab["Violação"].tolist()
            unidades = inviab["Unidade"].tolist()
            nome = f"{caso.ano}_{str(caso.mes).zfill(2)}_rv{caso.revisao}"
            dados_inviab["Caso"] = [nome] * len(flexibilizacoes)
            dados_inviab["Flexibilizacao"] = flexibilizacoes
            dados_inviab["Estagio"] = estagios
            dados_inviab["Cenario"] = cenarios
            dados_inviab["Restricao"] = restricoes
            dados_inviab["Violacao"] = violacoes
            dados_inviab["Unidade"] = unidades
            df_inviab = pd.DataFrame(data=dados_inviab)
            if inviabilidades_decomps.empty:
                inviabilidades_decomps = df_inviab
            else:
                inviabilidades_decomps = pd.concat(
                    [inviabilidades_decomps, df_inviab], ignore_index=True
                )
            return inviabilidades_decomps

        if isinstance(caso, CasoNEWAVE):
            pass
        elif isinstance(caso, CasoDECOMP):
            d = le_inviabilidades_decomps(inviabilidades_decomps, caso)
            inviabilidades_decomps = d
        else:
            raise ValueError(
                f"Caso do tipo {type(caso)} não suportado"
                + "na síntese do estudo"
            )
        return inviabilidades_newaves, inviabilidades_decomps

    def resume_dados_casos(self, casos: List[Caso]):

        resumo_estados = pd.DataFrame(columns=DadosEstudo.COLUNAS_ESTADO)
        resumo_newaves = pd.DataFrame(columns=DadosEstudo.COLUNAS_CUSTOS)
        resumo_decomps = pd.DataFrame(columns=DadosEstudo.COLUNAS_RESUMO)
        resumo_reservatorios = pd.DataFrame()
        cols_conv = DadosEstudo.COLUNAS_CONVERGENCIA
        cols_inviab = DadosEstudo.COLUNAS_INVIABS
        convergencias_newaves = pd.DataFrame(columns=cols_conv)
        convergencias_decomps = pd.DataFrame(columns=cols_conv)
        inviabilidades_newaves = pd.DataFrame(columns=cols_inviab)
        inviabilidades_decomps = pd.DataFrame(columns=cols_inviab)
        for i, c in enumerate(casos):
            if c.estado == EstadoCaso.CONCLUIDO:
                # Passa i == 1 para significar o primeiro DECOMP (segundo caso)
                e, n, d, r = DadosEstudo.__le_resumo(
                    c,
                    resumo_estados,
                    resumo_newaves,
                    resumo_decomps,
                    resumo_reservatorios,
                    i == 1,
                )
                resumo_estados = e
                resumo_newaves = n
                resumo_decomps = d
                resumo_reservatorios = r
                n, d = DadosEstudo.__le_convergencia(
                    c, convergencias_newaves, convergencias_decomps
                )
                convergencias_newaves = n
                convergencias_decomps = d
                n, d = DadosEstudo.__le_inviabilidades(
                    c, inviabilidades_newaves, inviabilidades_decomps
                )
                inviabilidades_newaves = n
                inviabilidades_decomps = d

        self._resumo_estados = resumo_estados
        self._resumo_newaves = resumo_newaves
        self._resumo_decomps = resumo_decomps
        self._resumo_reservatorios = resumo_reservatorios
        self._convergencias_newaves = convergencias_newaves
        self._convergencias_decomps = convergencias_decomps
        self._inviabilidades_decomps = inviabilidades_decomps

    @property
    def resumo_estados(self) -> pd.DataFrame:
        return self._resumo_estados

    @property
    def resumo_newaves(self) -> pd.DataFrame:
        return self._resumo_newaves

    @property
    def resumo_decomps(self) -> pd.DataFrame:
        return self._resumo_decomps

    @property
    def resumo_reservatorios(self) -> pd.DataFrame:
        return self._resumo_reservatorios

    @property
    def convergencias_newaves(self) -> pd.DataFrame:
        return self._convergencias_newaves

    @property
    def convergencias_decomps(self) -> pd.DataFrame:
        return self._convergencias_decomps

    @property
    def inviabilidades_decomps(self) -> pd.DataFrame:
        return self._inviabilidades_decomps
