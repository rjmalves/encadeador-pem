from idecomp.decomp.relato import Relato
from idecomp.decomp.relgnl import Relgnl
import pandas as pd  # type: ignore


class ProcessadorDecomp:
    @staticmethod
    def gt_percentual(
        relato: Relato, relgnl: Relgnl, col: str
    ) -> pd.DataFrame:
        def extrai_gts_semana(relato: Relato, semana: int) -> pd.DataFrame:
            gt: pd.DataFrame = relato.geracao_termica_submercado
            gt = gt[["nome_submercado", f"estagio_{semana}"]]
            gt = gt.set_index("nome_submercado")
            gt = gt.drop(index="FC")
            return gt

        def extrai_gts_min_max_semana(
            relato: Relato, relgnl: Relgnl, semana: int
        ) -> pd.DataFrame:
            merc: pd.DataFrame = relato.dados_mercado
            term: pd.DataFrame = relato.dados_termicas
            termg: pd.DataFrame = relgnl.usinas_termicas
            disp: pd.DataFrame = relato.disponibilidades_termicas

            # Filtra somente as informações de Sª semana do relato e do
            # relgnl
            term_1s = term.loc[term["estagio"] == semana, :].copy()
            term_1s_gnl = termg.loc[termg["estagio"] == semana, :].copy()
            term_1s_gnl.columns = term_1s.columns
            term_1s = pd.concat([term_1s, term_1s_gnl], ignore_index=True)
            term_1s
            merc_1s = merc.loc[merc["estagio"] == semana, :].copy()
            merc_1s
            # Considera as disponibilidades das térmicas nos
            # GTmin e GTmax
            for idx, lin in term_1s.iterrows():
                filtro = disp["codigo_usina"] == int(lin["codigo_usina"])
                taxa_disp = (
                    float(disp.loc[filtro, f"estagio_{semana}"]) / 100.0
                )
                for pat in [1, 2, 3]:
                    c_min = f"geracao_minima_patamar_{pat}"
                    c_max = f"geracao_maxima_patamar_{pat}"
                    gt_min = term_1s.loc[idx, c_min]
                    gt_max = term_1s.loc[idx, c_max]
                    term_1s.loc[idx, c_min] = taxa_disp * float(gt_min)
                    term_1s.loc[idx, c_max] = taxa_disp * float(gt_max)

            # Faz o cálculo de GTmin e GTmax agrupando os patamares com
            # as durações
            cmi = "geracao_minima"
            cma = "geracao_maxima"
            subs = term_1s["nome_submercado"].unique()
            for s in subs:
                fil = term_1s["nome_submercado"] == s
                dp1 = float(
                    merc_1s.loc[merc_1s["nome_submercado"] == s, "patamar_1"]
                )
                dp2 = float(
                    merc_1s.loc[merc_1s["nome_submercado"] == s, "patamar_2"]
                )
                dp3 = float(
                    merc_1s.loc[merc_1s["nome_submercado"] == s, "patamar_3"]
                )
                dur_total = dp1 + dp2 + dp3
                dp1 /= dur_total
                dp2 /= dur_total
                dp3 /= dur_total
                term_1s.loc[fil, cmi] = (
                    term_1s.loc[fil, "geracao_minima_patamar_1"] * dp1
                    + term_1s.loc[fil, "geracao_minima_patamar_2"] * dp2
                    + term_1s.loc[fil, "geracao_minima_patamar_3"] * dp3
                )
                term_1s.loc[fil, cma] = (
                    term_1s.loc[fil, "geracao_maxima_patamar_1"] * dp1
                    + term_1s.loc[fil, "geracao_maxima_patamar_2"] * dp2
                    + term_1s.loc[fil, "geracao_maxima_patamar_3"] * dp3
                )

            # Obtém o GTmin e GTmax por subsistema
            term_grupo = term_1s.groupby("nome_submercado").sum()[[cmi, cma]]
            return term_grupo

        def extrai_gt_percentual_semana(
            relato: Relato, relgnl: Relgnl, semana: int
        ) -> pd.DataFrame:
            df_gt = extrai_gts_semana(relato, semana)
            df_gt_min_max = extrai_gts_min_max_semana(relato, relgnl, semana)
            df_gt_min_max["geracao"] = df_gt[f"estagio_{semana}"].copy()
            df_gt_min_max["nome_submercado"] = df_gt_min_max.index
            df_gt_min_max = df_gt_min_max[
                [
                    "nome_submercado",
                    "geracao_minima",
                    "geracao",
                    "geracao_maxima",
                ]
            ]
            df_gt_min_max = df_gt_min_max.reset_index(drop=True)
            # Adiciona dados totais para o SIN
            soma_gt = df_gt_min_max.sum(axis=0)
            df_gt_min_max.loc[4, "nome_submercado"] = "SIN"
            df_gt_min_max.loc[4, "geracao_minima"] = float(
                soma_gt.loc["geracao_minima"]
            )
            df_gt_min_max.loc[4, "geracao"] = float(soma_gt.loc["geracao"])
            df_gt_min_max.loc[4, "geracao_maxima"] = float(
                soma_gt.loc["geracao_maxima"]
            )
            return df_gt_min_max

        df_completo = pd.DataFrame()
        vols: pd.DataFrame = relato.volume_util_reservatorios
        n_semanas = len(list(vols.columns)) - 3
        for i in range(1, n_semanas + 1):
            df = extrai_gt_percentual_semana(relato, relgnl, i)
            df["estagio"] = i
            if df_completo.empty:
                df_completo = df
            else:
                df_completo = pd.concat([df_completo, df], ignore_index=True)

        # Reordena as columas do dataframe
        cols_sem_estagio = [c for c in df_completo.columns if c != "estagio"]
        dfc = df_completo[["estagio"] + cols_sem_estagio].copy()

        dfc.loc[:, "geracao_percentual_maxima"] = (
            100 * dfc["geracao"] / dfc["geracao_maxima"]
        )
        dfc.loc[:, "geracao_percentual_flexivel"] = 100 * (
            (dfc["geracao"] - dfc["geracao_minima"])
            / (dfc["geracao_maxima"] - dfc["geracao_minima"])
        )

        # Transforma só para o DF com percentual da máxima, no padrão
        # das demais variáveis
        estagios = dfc["estagio"].unique()
        subsistemas = dfc["nome_submercado"].unique()
        cols = ["nome_submercado"] + [f"estagio_{e}" for e in estagios]
        df_final = pd.DataFrame(columns=cols)
        for i, s in enumerate(subsistemas):
            df_final.loc[i, "nome_submercado"] = s
            gt = dfc.loc[dfc["nome_submercado"] == s, col].to_numpy()
            df_final.loc[i, [f"estagio_{e}" for e in estagios]] = gt

        return df_final

    @staticmethod
    def gt_percentual_maxima(relato: Relato, relgnl: Relgnl):
        return ProcessadorDecomp.gt_percentual(
            relato, relgnl, "geracao_percentual_maxima"
        )

    @staticmethod
    def gt_percentual_flexivel(relato: Relato, relgnl: Relgnl):
        return ProcessadorDecomp.gt_percentual(
            relato, relgnl, "geracao_percentual_flexivel"
        )
