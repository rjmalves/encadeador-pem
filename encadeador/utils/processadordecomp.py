from idecomp.decomp.relato import Relato
from idecomp.decomp.relgnl import RelGNL
import pandas as pd


class ProcessadorDecomp:
    @staticmethod
    def gt_percentual(relato: Relato, relgnl: RelGNL, col: str) -> pd.DataFrame:
        def extrai_gts_semana(relato: Relato, semana: int) -> pd.DataFrame:
            gt = relato.geracao_termica_subsistema[
                ["Subsistema", f"Estágio {semana}"]
            ]
            gt = gt.set_index("Subsistema")
            gt = gt.drop(index="FC")
            return gt

        def extrai_gts_min_max_semana(
            relato: Relato, relgnl: RelGNL, semana: int
        ) -> pd.DataFrame:
            merc = relato.dados_mercado
            term = relato.dados_termicas
            termg = relgnl.usinas_termicas
            disp = relato.disponibilidades_termicas

            # Filtra somente as informações de Sª semana do relato e do
            # relgnl
            term_1s = term.loc[term["Estágio"] == semana, :].copy()
            term_1s_gnl = termg.loc[termg["Estágio"] == semana, :].copy()
            term_1s_gnl.columns = term_1s.columns
            term_1s = pd.concat([term_1s, term_1s_gnl], ignore_index=True)
            term_1s
            merc_1s = merc.loc[merc["Estágio"] == semana, :].copy()
            merc_1s
            # Considera as disponibilidades das térmicas nos
            # GTmin e GTmax
            for idx, lin in term_1s.iterrows():
                filtro = disp["Código"] == int(lin["Código"])
                taxa_disp = float(disp.loc[filtro, f"Estágio {semana}"]) / 100.0
                for pat in [1, 2, 3]:
                    c_min = f"GT Min Pat. {pat}"
                    c_max = f"GT Max Pat. {pat}"
                    gt_min = term_1s.loc[idx, c_min]
                    gt_max = term_1s.loc[idx, c_max]
                    term_1s.loc[idx, c_min] = taxa_disp * float(gt_min)
                    term_1s.loc[idx, c_max] = taxa_disp * float(gt_max)

            # Faz o cálculo de GTmin e GTmax agrupando os patamares com
            # as durações
            cmi = "GT Min"
            cma = "GT Max"
            subs = term_1s["Subsistema"].unique()
            for s in subs:
                fil = term_1s["Subsistema"] == s
                dp1 = float(
                    merc_1s.loc[merc_1s["Subsistema"] == s, "Patamar 1"]
                )
                dp2 = float(
                    merc_1s.loc[merc_1s["Subsistema"] == s, "Patamar 2"]
                )
                dp3 = float(
                    merc_1s.loc[merc_1s["Subsistema"] == s, "Patamar 3"]
                )
                dur_total = dp1 + dp2 + dp3
                dp1 /= dur_total
                dp2 /= dur_total
                dp3 /= dur_total
                term_1s.loc[fil, cmi] = (
                    term_1s.loc[fil, "GT Min Pat. 1"] * dp1
                    + term_1s.loc[fil, "GT Min Pat. 2"] * dp2
                    + term_1s.loc[fil, "GT Min Pat. 3"] * dp3
                )
                term_1s.loc[fil, cma] = (
                    term_1s.loc[fil, "GT Max Pat. 1"] * dp1
                    + term_1s.loc[fil, "GT Max Pat. 2"] * dp2
                    + term_1s.loc[fil, "GT Max Pat. 3"] * dp3
                )

            # Obtém o GTmin e GTmax por subsistema
            term_grupo = term_1s.groupby("Subsistema").sum()[[cmi, cma]]
            return term_grupo

        def extrai_gt_percentual_semana(
            relato: Relato, relgnl: RelGNL, semana: int
        ) -> pd.DataFrame:
            df_gt = extrai_gts_semana(relato, semana)
            df_gt_min_max = extrai_gts_min_max_semana(relato, relgnl, semana)
            df_gt_min_max["GT"] = df_gt[f"Estágio {semana}"].copy()
            df_gt_min_max["Subsistema"] = df_gt_min_max.index
            df_gt_min_max = df_gt_min_max[
                ["Subsistema", "GT Min", "GT", "GT Max"]
            ]
            df_gt_min_max = df_gt_min_max.reset_index(drop=True)
            # Adiciona dados totais para o SIN
            soma_gt = df_gt_min_max.sum(axis=0)
            df_gt_min_max.loc[4, "Subsistema"] = "SIN"
            df_gt_min_max.loc[4, "GT Min"] = float(soma_gt.loc["GT Min"])
            df_gt_min_max.loc[4, "GT"] = float(soma_gt.loc["GT"])
            df_gt_min_max.loc[4, "GT Max"] = float(soma_gt.loc["GT Max"])
            return df_gt_min_max

        df_completo = None
        n_semanas = len(list(relato.volume_util_reservatorios.columns)) - 3
        for i in range(1, n_semanas + 1):
            df = extrai_gt_percentual_semana(relato, relgnl, i)
            df["Estágio"] = i
            if df_completo is None:
                df_completo = df
            else:
                df_completo = pd.concat([df_completo, df], ignore_index=True)

        # Reordena as columas do dataframe
        cols_sem_estagio = [c for c in df_completo.columns if c != "Estágio"]
        dfc = df_completo[["Estágio"] + cols_sem_estagio].copy()

        dfc.loc[:, "GT Percentual Max"] = 100 * dfc["GT"] / dfc["GT Max"]
        dfc.loc[:, "GT Percentual Flex"] = 100 * (
            (dfc["GT"] - dfc["GT Min"]) / (dfc["GT Max"] - dfc["GT Min"])
        )

        # Transforma só para o DF com percentual da máxima, no padrão
        # das demais variáveis
        estagios = dfc["Estágio"].unique()
        subsistemas = dfc["Subsistema"].unique()
        cols = ["Subsistema"] + [f"Estágio {e}" for e in estagios]
        df_final = pd.DataFrame(columns=cols)
        for i, s in enumerate(subsistemas):
            df_final.loc[i, "Subsistema"] = s
            gt = dfc.loc[dfc["Subsistema"] == s, col].to_numpy()
            df_final.loc[i, [f"Estágio {e}" for e in estagios]] = gt

        return df_final

    @staticmethod
    def gt_percentual_maxima(relato: Relato, relgnl: RelGNL):
        return ProcessadorDecomp.gt_percentual(
            relato, relgnl, "GT Percentual Max"
        )

    @staticmethod
    def gt_percentual_flexivel(relato: Relato, relgnl: RelGNL):
        return ProcessadorDecomp.gt_percentual(
            relato, relgnl, "GT Percentual Flex"
        )
