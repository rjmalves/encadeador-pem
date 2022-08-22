from encadeador.modelos.caso2 import Caso
from encadeador.modelos.programa import Programa
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.services.unitofwork.newave import factory as nw_uow_factory
from encadeador.services.unitofwork.decomp import factory as dc_uow_factory
from encadeador.utils.log import Log

from inewave.newave.modelos.modif import USINA, VAZMINT, VAZMIN
from inewave.newave import Modif, RE
from idecomp.decomp.modelos.dadger import SB, HQ, LQ, CQ, DP
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.relato import Relato
from abc import abstractmethod
from typing import List, Dict, Optional
import pandas as pd  # type: ignore
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta  # type: ignore


class AplicadorRegrasReservatorios:
    def __init__(self, caso: Caso) -> None:
        self._caso = caso

    @staticmethod
    def factory(caso: Caso) -> "AplicadorRegrasReservatorios":
        if caso.programa == Programa.NEWAVE:
            return AplicadorRegrasReservatoriosNEWAVE(caso)
        elif caso.programa == Programa.DECOMP:
            return AplicadorRegrasReservatoriosDECOMP(caso)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    def regras_mes(
        self, regras: List[RegraReservatorio], mes: int
    ) -> List[RegraReservatorio]:
        return list(set([r for r in regras if r.mes == mes]))

    def converte_regra_hm3(
        self, regra: RegraReservatorio, tabela_hidr: pd.DataFrame
    ) -> RegraReservatorio:
        regra_convertida = RegraReservatorio(
            regra.codigo_reservatorio,
            regra.codigo_usina,
            regra.tipo_restricao,
            regra.mes,
            0.0,
            0.0,
            0.0,
            0.0,
            regra.periodicidade,
            regra.legenda_faixa,
        )
        vmin = float(
            tabela_hidr.loc[
                tabela_hidr.index == regra.codigo_reservatorio, "Volume Mínimo"
            ]
        )
        vmax = float(
            tabela_hidr.loc[
                tabela_hidr.index == regra.codigo_reservatorio, "Volume Máximo"
            ]
        )
        vutil = vmax - vmin
        regra_convertida.volume_minimo = (
            vmin + vutil * regra.volume_minimo / 100.0
        )
        regra_convertida.volume_maximo = (
            vmin + vutil * regra.volume_maximo / 100.0
        )
        return regra_convertida

    def converte_volumes_relato_hm3(
        self, tabela_relato: pd.DataFrame, tabela_hidr: pd.DataFrame
    ):
        tabela_convertida = tabela_relato.copy()
        cols_estagios = ["Inicial"] + [
            c for c in tabela_convertida.columns if "Estágio" in c
        ]
        for _, linha in tabela_relato.iterrows():
            vmin = float(
                tabela_hidr.loc[
                    tabela_hidr.index == int(linha["Número"]), "Volume Mínimo"
                ]
            )
            vmax = float(
                tabela_hidr.loc[
                    tabela_hidr.index == int(linha["Número"]), "Volume Máximo"
                ]
            )
            vutil = vmax - vmin
            for c in cols_estagios:
                v = float(linha[c]) * vutil / 100.0 + vmin
                tabela_convertida.loc[
                    tabela_convertida["Número"] == int(linha["Número"]), c
                ] = v
        return tabela_convertida

    def agrupa_usinas_defluencia(
        self, regras: List[RegraReservatorio]
    ) -> List[RegraReservatorio]:
        # TODO - PREMISSA:
        # Assume-se que os reservatórios que compõe o equivalente
        # para cálculo do limite de defluência tem os mesmos limites
        # superiores e inferiores de defluência.
        regras_agrupadas: List[RegraReservatorio] = []
        usinas_defluencia = list(set([r.codigo_usina for r in regras]))
        for u in usinas_defluencia:
            faixas_usina = list(
                set([r._legenda_faixa for r in regras if r.codigo_usina == u])
            )
            for f in faixas_usina:
                periodicidades = list(
                    set(
                        [
                            r._periodicidade
                            for r in regras
                            if r.codigo_usina == u and r._legenda_faixa == f
                        ]
                    )
                )
                for p in periodicidades:
                    regra_usina = RegraReservatorio(
                        [], u, "QDEF", 0, 0.0, 0.0, 0.0, 0.0, p, f
                    )
                    regras_individuais = [
                        r
                        for r in regras
                        if r.codigo_usina == u
                        and r._legenda_faixa == f
                        and r._periodicidade == p
                    ]
                    for r in regras_individuais:
                        regra_usina.codigo_reservatorio.append(
                            r.codigo_reservatorio
                        )
                        regra_usina.volume_minimo += r.volume_minimo
                        regra_usina.volume_maximo += r.volume_maximo
                        regra_usina.limite_minimo = r.limite_minimo
                        regra_usina.limite_maximo = r.limite_maximo
                        regra_usina.mes = r.mes
                        regra_usina.mes = r.mes
                    regra_usina.codigo_reservatorio = list(
                        set(regra_usina.codigo_reservatorio)
                    )
                    regras_agrupadas.append(regra_usina)
        return regras_agrupadas

    @abstractmethod
    def aplica_regras(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
        pass


class AplicadorRegrasReservatoriosNEWAVE(AplicadorRegrasReservatorios):
    def __init__(self, caso: Caso) -> None:
        super().__init__(caso)

    MAPA_FICTICIAS_MODIF: Dict[int, List[int]] = {
        156: [156, 295],
        178: [172, 176, 178],
    }
    MAPA_FICTICIAS_RE: Dict[int, List[int]] = {
        156: [156],
        178: [172, 176, 178],
    }

    def identifica_regra_ativa(
        self,
        regras: List[RegraReservatorio],
        codigo_usina: int,
        volumes: pd.DataFrame,
        estagio: int,
    ) -> Optional[RegraReservatorio]:

        codigo_reservatorio = next(
            r.codigo_reservatorio
            for r in regras
            if r.codigo_usina == codigo_usina
        )
        volume_total = float(
            volumes.loc[
                volumes["Número"].isin(codigo_reservatorio),
                f"Estágio {estagio}",
            ].sum()
        )
        try:
            regra = None
            for r in regras:
                if all(
                    [
                        r.codigo_usina == codigo_usina,
                        r.volume_minimo
                        <= float(volume_total)
                        < r.volume_maximo,
                    ]
                ):
                    regra = r
                    break
            if regra is None:
                raise StopIteration()
        except StopIteration:
            Log.log().warning(
                "Não foi encontrada regra de operação ativa "
                + f"para a usina {codigo_usina} "
                + f"(reservatórios {codigo_reservatorio}) "
                + f"no volume {float(volume_total)}"
            )
            regra = None
        return regra

    def regras_estagios(
        self,
        regras: List[RegraReservatorio],
        mapa_estagio_dia: Dict[int, date],
    ) -> Dict[int, List[RegraReservatorio]]:
        regras_mapeadas: Dict[int, List[RegraReservatorio]] = {}
        for estagio, dia_fim in mapa_estagio_dia.items():
            regras_mapeadas[estagio] = list(
                set([r for r in regras if r.mes == dia_fim.month])
            )
        return regras_mapeadas

    def identifica_regras_ativas(
        self,
        regras: List[RegraReservatorio],
        volumes_hm3: pd.DataFrame,
    ) -> Dict[int, List[RegraReservatorio]]:
        # Obtém os volumes
        regras_ativas_estagios: Dict[int, List[RegraReservatorio]] = {}
        estagio = int(
            [c for c in list(volumes_hm3.columns) if "Estágio" in c][-1].split(
                "Estágio"
            )[1]
        )
        # Obtém as regras ativas para cada usina
        usinas_com_restricao = list(set([r.codigo_usina for r in regras]))
        regras_ativas: List[RegraReservatorio] = []
        for u in usinas_com_restricao:
            regra_estagio = self.identifica_regra_ativa(
                regras, u, volumes_hm3, estagio
            )
            if regra_estagio is not None:
                regras_ativas.append(regra_estagio)
        regras_ativas_estagios[estagio] = regras_ativas
        return regras_ativas_estagios

    def obtem_ghmax_usina(
        self, codigo: int, qdef: float, hidr: pd.DataFrame
    ) -> float:
        def aplica_polinomio(coeficientes: List, vol: float) -> float:
            return sum([c * vol**i for i, c in enumerate(coeficientes)])

        # Localiza os dados de interesse da usina
        volmin = float(hidr.loc[codigo, "Volume Mínimo"])
        volmax = float(hidr.loc[codigo, "Volume Máximo"])
        volutil = volmax - volmin
        vol65 = volmin + 0.65 * volutil
        hjus = float(hidr.loc[codigo, "Canal de Fuga Médio"])
        hmon = aplica_polinomio(
            [float(hidr.loc[codigo, f"A{i} CV"]) for i in range(5)], vol65
        )
        perdas = float(hidr.loc[codigo, "Perdas"])
        hliq = hmon - hjus - perdas
        prod = float(hidr.loc[codigo, "Produtibilidade Específica"])
        prod_media = prod * hliq
        return prod_media * qdef

    def aplica_regra_qdef_modif(
        self,
        regra: RegraReservatorio,
        modif: Modif,
        hidr: pd.DataFrame,
        codigo: int = None,
    ):
        if codigo is None:
            codigo = regra.codigo_usina
        # Se a regra não tem limite mínimo, ignora
        if regra.limite_minimo is None:
            return

        modif_usina = modif.modificacoes_usina(codigo)
        # Se a usina em questão não é modificada, cria uma modificação nova
        if modif_usina is None:
            nova_usina = USINA()
            nova_usina.codigo = codigo
            nova_usina.nome = str(hidr.loc[codigo, "Nome"])
            modif.append_registro(nova_usina)
            Log.log().info(f"Criando novo registro USINA {codigo}")
        # Obtém o registro que modifica a usina
        usina: USINA = modif.usina(codigo=codigo)

        vazmint_existentes = [m for m in modif_usina if isinstance(m, VAZMINT)]
        vazmin_existentes = [m for m in modif_usina if isinstance(m, VAZMIN)]
        Log.log().info(
            f"Existem {len(vazmint_existentes)} VAZMINT"
            + f" para a usina {codigo}"
        )
        Log.log().info(
            f"Existem {len(vazmin_existentes)} VAZMIN"
            + f" para a usina {codigo}"
        )
        # Guarda a vazão do primeiro VAZMINT que tenha início após os
        # 2 primeiros meses. Se não existir, procura VAZMIN. Por último,
        # procura no HIDR
        data_caso = date(self._caso.ano, self._caso.mes, 1)
        ultima_vazao = 0.0
        if len(vazmint_existentes) > 0:
            for m in vazmint_existentes:
                ultima_vazao = m.vazao
                data_inicio = date(m.ano, m.mes, 1)
                if data_inicio >= data_caso + relativedelta(months=+2):
                    break
            if ultima_vazao == 0:
                ultima_vazao = float(hidr.loc[codigo, "Vazão Mínima"])
        elif len(vazmin_existentes) > 0:
            ultima_vazao = vazmin_existentes[-1].vazao
        else:
            ultima_vazao = float(hidr.loc[codigo, "Vazão Mínima"])
        Log.log().info(f"Última vazão = {ultima_vazao}")
        for m in vazmint_existentes:
            # Deleta os VAZMINT que iniciem nos 2 primeiros meses
            data_inicio = date(m.ano, m.mes, 1)
            if data_inicio < data_caso + relativedelta(months=+2):
                modif.deleta_registro(m)
        # Cria os VAZMINT
        # - O primeiro é válido para os 2 primeiros meses
        novo_vazmint = VAZMINT()
        novo_vazmint.mes = self._caso.mes
        novo_vazmint.ano = self._caso.ano
        novo_vazmint.vazao = regra.limite_minimo
        Log.log().info(
            f"Criando VAZMINT = {self._caso.mes}"
            + f" {self._caso.ano} {regra.limite_minimo}"
        )
        modif.cria_registro(usina, novo_vazmint)
        # - O segundo é para retornar ao valor anterior
        fim_vazmint = date(
            year=self._caso.ano, month=self._caso.mes, day=1
        ) + relativedelta(months=+2)
        prox_vazmint = VAZMINT()
        prox_vazmint.mes = fim_vazmint.month
        prox_vazmint.ano = fim_vazmint.year
        prox_vazmint.vazao = ultima_vazao
        Log.log().info(
            f"Criando VAZMINT = {fim_vazmint.month}"
            + f" {fim_vazmint.year} {ultima_vazao}"
        )
        modif.cria_registro(novo_vazmint, prox_vazmint)

    def aplica_regra_qdef_re(
        self,
        regra: RegraReservatorio,
        re: RE,
        hidr: pd.DataFrame,
        codigo: int = None,
    ):
        if codigo is None:
            codigo = regra.codigo_usina
        # Se não existe um conjunto com a usina em questão, cria.
        cols_usinas = [f"Usina {i}" for i in range(1, 11)]
        df_conjuntos = re.usinas_conjuntos
        conjuntos = list(df_conjuntos["Conjunto"].unique())
        if codigo not in df_conjuntos[cols_usinas].to_numpy():
            Log.log().info(f"Criando conjunto com usina {codigo}")
            num_conjunto = max(conjuntos) + 1
            novo_conjunto = {
                **{"Conjunto": [num_conjunto]},
                **{c: [0] for c in cols_usinas},
            }
            novo_conjunto["Usina 1"] = [codigo]
            re.usinas_conjuntos = df_conjuntos.append(
                pd.DataFrame(data=novo_conjunto), ignore_index=True
            )
            df_conjuntos = re.usinas_conjuntos
        # Senão, identifica.
        num_conjunto = next(
            int(linha["Conjunto"])
            for _, linha in df_conjuntos.iterrows()
            if codigo in linha[cols_usinas].to_numpy()
        )
        # Cria as restrições para o conjunto em questão, nos 2 primeiros
        # meses do horizonte
        mes_inicial = date(year=self._caso.ano, month=self._caso.mes, day=1)
        mes_final = mes_inicial + relativedelta(months=+1)
        # Deleta as restrições do conjunto em questão, se existirem e começarem
        # em algum dos 2 primeiros meses
        restricoes = re.restricoes
        indices_restricoes = restricoes.loc[
            (restricoes["Conjunto"] == num_conjunto)
            & (
                (restricoes["Mês Início"] == mes_inicial.month)
                | (restricoes["Mês Início"] == mes_final.month)
            )
            & (
                (restricoes["Ano Início"] == mes_inicial.year)
                | (restricoes["Ano Início"] == mes_final.year)
            ),
            :,
        ].index
        restricoes = restricoes.drop(index=indices_restricoes)

        # Se a regra não tem limite máximo, ignora
        qdef = regra.limite_maximo
        if qdef is None:
            return
        nova_restricao = {
            "Conjunto": [num_conjunto],
            "Mês Início": [mes_inicial.month],
            "Ano Início": [mes_inicial.year],
            "Mês Fim": [mes_final.month],
            "Ano Fim": [mes_final.year],
            "Flag P": [0],
            "Restrição": [self.obtem_ghmax_usina(codigo, qdef, hidr)],
            "Motivo": ["REGRA ANA"],
        }
        re.restricoes = restricoes.append(
            pd.DataFrame(data=nova_restricao), ignore_index=True
        )

    def aplica_regra(
        self,
        regra: RegraReservatorio,
        hidr: pd.DataFrame,
        modif: Modif,
        re: RE,
    ) -> bool:
        if regra.tipo_restricao == "QDEF":
            Log.log().info(
                f"Aplicando regra: {str(regra)} no mês {self._caso.mes}"
            )
            # No caso de existirem, aplica também nas fictícias
            # Aplica a restrição da defluência mínima, se houver,
            # no modif.dat
            mapa_modif = (
                AplicadorRegrasReservatoriosNEWAVE.MAPA_FICTICIAS_MODIF
            )
            if regra.limite_minimo is not None:
                if regra.codigo_usina in mapa_modif.keys():
                    for codigo in mapa_modif[regra.codigo_usina]:
                        self.aplica_regra_qdef_modif(
                            regra, modif, hidr, codigo=codigo
                        )
                else:
                    self.aplica_regra_qdef_modif(regra, modif, hidr)
            # Aplica a restrição da defluência máxima, se houver,
            # no re.dat
            mapa_re = AplicadorRegrasReservatoriosNEWAVE.MAPA_FICTICIAS_RE
            if regra.limite_maximo is not None:
                if regra.codigo_usina in mapa_re.keys():
                    for codigo in mapa_re[regra.codigo_usina]:
                        self.aplica_regra_qdef_re(
                            regra, re, hidr, codigo=codigo
                        )
                else:
                    self.aplica_regra_qdef_re(regra, re, hidr)
        return True

    def aplica_regras(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
        # Obtém o último DECOMP executado no mês anterior
        try:
            mes_anterior = 12 if self._caso.mes == 1 else self._caso.mes - 1
            ultimo_decomp = next(
                c
                for c in reversed(casos_anteriores)
                if c.programa == Programa.DECOMP and c.mes == mes_anterior
            )
        except StopIteration:
            Log.log().info(
                f"Caso {self._caso.nome} não possui DECOMP anterior. "
                + "Não serão aplicadas regras operativas de reservatórios."
            )
            return True

        dc_uow = dc_uow_factory("FS", ultimo_decomp.caminho)
        with dc_uow:
            relato = dc_uow.decomp.get_relato()

        # Filtra as regras de operação para o mês do caso
        regras_mes = self.regras_mes(regras_operacao, self._caso.mes)

        nw_uow = nw_uow_factory("FS", self._caso.caminho)
        with nw_uow:
            cadastro_hidr = nw_uow.newave.get_hidr().cadastro

        # Converte as regras para hm3
        regras_hm3 = [
            self.converte_regra_hm3(r, cadastro_hidr) for r in regras_mes
        ]

        # Agrupa regras por usina com defluência limitada
        regras_agrupadas = self.agrupa_usinas_defluencia(regras_hm3)

        volumes_relato_hm3 = self.converte_volumes_relato_hm3(
            relato.volume_util_reservatorios, cadastro_hidr
        )
        # Identifica as regras ativas
        regras_ativas = self.identifica_regras_ativas(
            regras_agrupadas, volumes_relato_hm3
        )

        sucessos: List[bool] = []
        # Para o NEWAVE, são sempre tomadas as regras vigentes para os
        # volumes do últimos estágio semanal do último DECOMP do mês anterior
        estagio = sorted(list(regras_ativas.keys()))[-1]
        with nw_uow:
            modif = nw_uow.newave.get_modif()
            re = nw_uow.newave.get_modif()
            for r in regras_ativas[estagio]:
                sucessos.append(self.aplica_regra(r, cadastro_hidr, modif, re))
            nw_uow.newave.set_modif(modif)
            nw_uow.newave.set_re(re)
        return all(sucessos)


class AplicadorRegrasReservatoriosDECOMP(AplicadorRegrasReservatorios):
    def __init__(self, caso: Caso) -> None:
        super().__init__(caso)

    # Override
    def identifica_regra_ativa(
        self,
        regras: List[RegraReservatorio],
        codigo_usina: int,
        volumes: pd.DataFrame,
        estagio: int,
    ) -> Optional[RegraReservatorio]:
        codigos_reservatorios = next(
            r.codigo_reservatorio
            for r in regras
            if r.codigo_usina == codigo_usina
        )
        volume_total = float(
            volumes.loc[
                volumes["Número"].isin(codigos_reservatorios),
                f"Estágio {estagio}",
            ].sum()
        )
        try:
            regra = None
            for r in regras:
                if all(
                    [
                        r.codigo_usina == codigo_usina,
                        r.volume_minimo
                        <= float(volume_total)
                        < r.volume_maximo,
                    ]
                ):
                    regra = r
                    break
            if regra is None:
                raise StopIteration()
        except StopIteration:
            Log.log().warning(
                "Não foi encontrada regra de operação ativa "
                + f"para a usina {codigo_usina} "
                + f"(reservatórios {codigos_reservatorios}) "
                + f"no volume {float(volume_total)}"
            )
            regra = None
        return regra

    def identifica_regras_ativas(
        self,
        regras: Dict[int, List[RegraReservatorio]],
        volumes_hm3: pd.DataFrame,
    ) -> Dict[int, List[RegraReservatorio]]:
        # Obtém os volumes

        regras_ativas_estagios: Dict[int, List[RegraReservatorio]] = {}
        # Obtém as regras ativas para cada usina
        for estagio, regras_estagio in regras.items():
            usinas_com_restricao = list(
                set([r.codigo_usina for r in regras_estagio])
            )
            regras_ativas: List[RegraReservatorio] = []
            for u in usinas_com_restricao:
                regra_estagio = self.identifica_regra_ativa(
                    regras[estagio], u, volumes_hm3, estagio
                )
                if regra_estagio is not None:
                    regras_ativas.append(regra_estagio)
            regras_ativas_estagios[estagio] = regras_ativas
        return regras_ativas_estagios

    def aplica_regra(
        self,
        dadger: Dadger,
        regra: RegraReservatorio,
        estagio_aplicacao: int,
    ) -> bool:
        def aplica_regra_qdef(
            regra: RegraReservatorio, dadger: Dadger, estagio: int
        ):
            # Se vai aplicar uma regra em um determinado estágio
            # acessa a restrição em todos os estágios futuros, até
            # o limite, para garantir os valore serão mantidos.
            cqs: List[CQ] = dadger.cq()
            if isinstance(cqs, CQ):
                cqs = [cqs]
            if isinstance(cqs, list):
                cqs_usina = [c for c in cqs if c.uhe == regra.codigo_usina]
                if len(cqs_usina) > 0:
                    codigos_restricoes = [cq.restricao for cq in cqs_usina]
                else:
                    codigos_restricoes = [cqs[-1].restricao + 1]
                    cqs_usinas = [CQ()]
                    cqs_usinas[0].restricao = [
                        codigos_restricoes[0],
                        1,
                        regra.codigo_usina,
                        1.0,
                        regra.tipo_restricao,
                    ]
                efs = [
                    dadger.hq(codigo=codigo).estagio_final
                    for codigo in codigos_restricoes
                ]
            else:
                for cq_usina, codigo in zip(cqs_usina, codigos_restricoes):
                    # Se não existe o registro HQ, cria, junto com um LQ
                    registros_dp = dadger.lista_registros(DP)
                    num_subsistemas = len(dadger.lista_registros(SB))
                    ef = int(len(registros_dp) / num_subsistemas)
                    Log.log().info(f"Criando HQ {codigo} - 1 {ef}")
                    hq_novo = HQ()
                    hq_novo._dados = [codigo, 1, ef]
                    lq_novo = LQ()
                    lq_novo._dados = [codigo, 1] + [
                        0,
                        99999,
                        0,
                        99999,
                        0,
                        99999,
                    ]
                    dadger.cria_registro(dadger.ev, hq_novo)
                    dadger.cria_registro(hq_novo, lq_novo)
                    dadger.cria_registro(lq_novo, cq_usina)
                efs = [
                    dadger.hq(codigo).estagio_final
                    for codigo in codigos_restricoes
                ]

            for cq_usina, codigo, ef in zip(
                cqs_usina, codigos_restricoes, efs
            ):
                for e in range(estagio, ef + 1):
                    dadger.lq(codigo, e)
                # Aplica a regra no estágio devido, se tiver limites inf/sup
                if regra.limite_minimo is not None:
                    dadger.lq(codigo, estagio).limites_inferiores = [
                        regra.limite_minimo
                    ] * 3
                if regra.limite_maximo is not None:
                    dadger.lq(codigo, estagio).limites_superiores = [
                        regra.limite_maximo
                    ] * 3

        Log.log().info(
            f"Aplicando regra: {str(regra)} no estágio {estagio_aplicacao}"
        )
        # Se ocorrer algum erro, retorna False
        if regra.tipo_restricao == "QDEF":
            aplica_regra_qdef(regra, dadger, estagio_aplicacao)
        else:
            return False
        return True

    def mapeia_semanas_dias_fim(
        self, dadger: Dadger, relato: Relato, delta_inicial: int = 0
    ) -> Dict[int, date]:
        dt = dadger.dt
        dia_inicio_caso_atual = date(dt.ano, dt.mes, dt.dia)
        num_semanas_caso_anterior = (
            len(relato.volume_util_reservatorios.columns) - 3
        )
        return {
            i
            + 1: dia_inicio_caso_atual
            - timedelta(weeks=delta_inicial, days=1)
            + timedelta(weeks=i, days=0)
            for i in range(num_semanas_caso_anterior)
        }

    def regras_estagios(
        self,
        regras: List[RegraReservatorio],
        mapa_estagio_dia: Dict[int, date],
    ) -> Dict[int, List[RegraReservatorio]]:
        regras_mapeadas: Dict[int, List[RegraReservatorio]] = {}
        for estagio, dia_fim in mapa_estagio_dia.items():
            regras_mapeadas[estagio] = list(
                set([r for r in regras if r.mes == dia_fim.month])
            )
        return regras_mapeadas

    def aplica_regras_caso(
        self,
        regras_operacao: List[RegraReservatorio],
        dadger: Dadger,
        relato: Relato,
        gap_semanas: int = 0,
        regras_mensais: bool = False,
    ) -> bool:

        # Identifica o dia de fim de cada semana do DECOMP anterior
        mapa_dias_fim = self.mapeia_semanas_dias_fim(
            dadger, relato, gap_semanas
        )
        # Se está falando de regras mensais, não consulta semana a semana
        if regras_mensais:
            ultimo_estagio = list(mapa_dias_fim.keys())[-1]
            mapa_dias_fim = {1: mapa_dias_fim[ultimo_estagio]}
        Log.log().info(
            f"Dias de fim dos estágios do DECOMP anterior: {mapa_dias_fim}"
        )

        # Filtra as regras de operação para cada estágio
        # do DECOMP anterior
        regras_estagios = self.regras_estagios(regras_operacao, mapa_dias_fim)

        # Identifica as regras ativas
        regras_ativas = self.identifica_regras_ativas(regras_estagios, relato)

        # Aplica as regras ativas
        registros_dp = dadger.dp()
        num_subsistemas = len(dadger.sb())
        num_estagios = int(len(registros_dp) / num_subsistemas)
        estagios_decomp_atual = list(range(1, num_estagios + 1))
        sucessos: List[bool] = []
        for estagio in estagios_decomp_atual:
            Log.log().info(
                f"Aplicando regras de reservatórios no estágio {estagio}"
            )
            if estagio not in regras_ativas.keys():
                estagio_aplicacao = sorted(list(regras_ativas.keys()))[-1]
            else:
                estagio_aplicacao = estagio
            for r in regras_ativas[estagio_aplicacao]:
                sucessos.append(self.aplica_regra(dadger, r, estagio))

        return all(sucessos)

    def aplica_regras(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
        regras_semanais = list(
            set([r for r in regras_operacao if r.periodicidade == "S"])
        )
        dc_uow = dc_uow_factory("FS", self._caso.caminho)
        with dc_uow:
            dadger_caso = dc_uow.decomp.get_dadger()
        try:
            ultimo_decomp = next(
                c
                for c in reversed(casos_anteriores)
                if c.programa == Programa.DECOMP
            )
            ultimo_dc_uow = dc_uow_factory("FS", ultimo_decomp.caminho)
            with ultimo_dc_uow:
                relato_ultimo_dc = ultimo_dc_uow.decomp.get_relato()
            self.aplica_regras_caso(
                regras_semanais, dadger_caso, relato_ultimo_dc
            )
        except StopIteration:
            Log.log().info(
                f"Caso {self._caso.nome} não possui DECOMP anterior. "
                + "Não serão aplicadas regras operativas de reservatórios "
                + "com periodicidade semanal."
            )

        try:
            mes_anterior = 12 if self._caso.mes == 1 else self._caso.mes - 1
            ultimo_decomp_mes_anterior = next(
                c
                for c in reversed(casos_anteriores)
                if c.programa == Programa.DECOMP and c.mes == mes_anterior
            )
            regras_mensais = list(
                set([r for r in regras_operacao if r.periodicidade == "M"])
            )
            gap_semanas = (
                len(casos_anteriores)
                - casos_anteriores.index(ultimo_decomp_mes_anterior)
                - 2
            )
            self.aplica_regras_caso(
                regras_mensais,
                dadger_caso,
                relato_ultimo_dc,
                gap_semanas,
                True,
            )
        except StopIteration:
            Log.log().info(
                f"Caso {self._caso.nome} não possui DECOMP no mês anterior. "
                + "Não serão aplicadas regras operativas de reservatórios "
                + "com periodicidade mensal."
            )

        with dc_uow:
            dc_uow.decomp.set_dadger(dadger_caso)

        return True
