from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.utils.log import Log

from inewave.newave.modelos.modif import USINA, VAZMINT
from inewave.newave import Modif, RE
from idecomp.decomp.modelos.dadger import SB, HQ, LQ, CQ, DP
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.relato import Relato
from idecomp.decomp.hidr import Hidr
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
        if isinstance(caso, CasoNEWAVE):
            return AplicadorRegrasReservatoriosNEWAVE(caso)
        elif isinstance(caso, CasoDECOMP):
            return AplicadorRegrasReservatoriosDECOMP(caso)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    def regras_mes(
        self, regras: List[RegraReservatorio], mes: int
    ) -> List[RegraReservatorio]:
        return list(set([r for r in regras if r.mes == mes]))

    @abstractmethod
    def aplica_regras(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
        pass


class AplicadorRegrasReservatoriosNEWAVE(AplicadorRegrasReservatorios):
    def __init__(self, caso: CasoNEWAVE) -> None:
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
        # TODO - Assume que uma usina só olha para um reservatório.
        # Não suporta o caso de uma usina depender de mais
        # de um volume.
        codigo_reservatorio = next(
            r.codigo_reservatorio
            for r in regras
            if r.codigo_usina == codigo_usina
        )
        volume_estagio = float(
            volumes.loc[
                volumes["Número"] == codigo_reservatorio,
                f"Estágio {estagio}",
            ]
        )
        try:
            regra = None
            for r in regras:
                limsup = 100.1 if r.volume_maximo == 100.0 else r.volume_maximo
                if all(
                    [
                        r.codigo_usina == codigo_usina,
                        r.codigo_reservatorio == codigo_reservatorio,
                        r.volume_minimo <= float(volume_estagio) < limsup,
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
                + f"(reservatório {codigo_reservatorio}) "
                + f"no volume {float(volume_estagio)}"
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
        self, regras: List[RegraReservatorio], relato: Relato
    ) -> Dict[int, List[RegraReservatorio]]:
        # Obtém os volumes
        volumes = relato.volume_util_reservatorios

        regras_ativas_estagios: Dict[int, List[RegraReservatorio]] = {}
        estagio = int(
            [
                c
                for c in list(relato.volume_util_reservatorios.columns)
                if "Estágio" in c
            ][-1].split("Estágio")[1]
        )
        # Obtém as regras ativas para cada usina
        usinas_com_restricao = list(set([r.codigo_usina for r in regras]))
        regras_ativas: List[RegraReservatorio] = []
        for u in usinas_com_restricao:
            regra_estagio = self.identifica_regra_ativa(
                regras, u, volumes, estagio
            )
            if regra_estagio is not None:
                regras_ativas.append(regra_estagio)
        regras_ativas_estagios[estagio] = regras_ativas
        return regras_ativas_estagios

    def obtem_ghmax_usina(self, codigo: int, qdef: float) -> float:
        def aplica_polinomio(coeficientes: List, vol: float) -> float:
            return sum([c * vol**i for i, c in enumerate(coeficientes)])

        # Lê o hidr.dat do caso atual
        hidr = Hidr.le_arquivo(self._caso.caminho).tabela
        # Localiza os dados de interesse da usina
        volmin = float(hidr.loc[codigo, "Volume Mínimo"])
        volmax = float(hidr.loc[codigo, "Volume Máximo"])
        volutil = volmax - volmin
        vol65 = volmin + 0.65 * volutil
        hjus = float(hidr.loc[codigo, "Canal de Fuga Médio"])
        hmon = aplica_polinomio(
            [float(hidr.loc[codigo, f"C{i} CV"]) for i in range(1, 6)], vol65
        )
        perdas = float(hidr.loc[codigo, "Perdas"])
        hliq = hmon - hjus - perdas
        prod = float(hidr.loc[codigo, "Produtibilidade Específica"])
        prod_media = prod * hliq
        return prod_media * qdef

    def aplica_regra_qdef_modif(
        self, regra: RegraReservatorio, modif: Modif, codigo: int = None
    ):
        if codigo is None:
            codigo = regra.codigo_usina
        # Se a regra não tem limite mínimo, ignora
        if regra.limite_minimo is None:
            return
        hidr = Hidr.le_arquivo(self._caso.caminho).tabela
        nome_usina = str(hidr.loc[codigo, "Nome"])
        # Se a usina em questão não é modificada, cria uma modificação nova
        if not any([m.codigo == codigo for m in modif.usina]):

            def compare(r):
                return r._ordem

            ultimo_registro = sorted(modif._registros, key=compare)[-1]
            nova_usina = USINA()
            nova_usina.codigo = codigo
            nova_usina.nome = nome_usina
            modif.cria_registro(ultimo_registro, nova_usina)
            Log.log().info(
                f"Criando novo registro USINA {codigo} após {ultimo_registro.mnemonico}"
            )
        # Obtém o registro que modifica a usina
        usina = next(m for m in modif.usina if m.codigo == codigo)
        # Obtém o próximo registro de usina
        idx_usina = usina._ordem
        if modif.usina.index(usina) == len(modif.usina) - 1:
            idx_proxima_usina = 99999.0
        else:
            idx_proxima_usina = modif.usina[
                modif.usina.index(usina) + 1
            ]._ordem
        vazmint_existentes = [
            m
            for m in modif.vazmint
            if idx_usina < m._ordem < idx_proxima_usina
        ]
        vazmin_existentes = [
            m for m in modif.vazmin if idx_usina < m._ordem < idx_proxima_usina
        ]
        Log.log().info(
            f"Existem {len(vazmint_existentes)} VAZMINT"
            + f" entre os registros {idx_usina} e {idx_proxima_usina}"
        )
        Log.log().info(
            f"Existem {len(vazmin_existentes)} VAZMIN"
            + f" entre os registros {idx_usina} e {idx_proxima_usina}"
        )
        # Guarda a vazão do primeiro VAZMINT que tenha início após os
        # 2 primeiros meses. Se não existir, procura VAZMIN. Por último,
        # procura no HIDR
        data_caso = date(self._caso.ano, self._caso.mes, 1)
        if len(vazmint_existentes) > 0:
            for m in vazmint_existentes:
                data_inicio = date(m.ano, m.mes, 1)
                if data_inicio >= data_caso + relativedelta(months=+2):
                    ultima_vazao = m.vazao
                    break
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
        self, regra: RegraReservatorio, re: RE, codigo: int = None
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
            "Restrição": [self.obtem_ghmax_usina(codigo, qdef)],
            "Motivo": ["REGRA ANA"],
        }
        re.restricoes = restricoes.append(
            pd.DataFrame(data=nova_restricao), ignore_index=True
        )

    def aplica_regra(self, regra: RegraReservatorio) -> bool:
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
                modif = Modif.le_arquivo(self._caso.caminho)
                if regra.codigo_usina in mapa_modif.keys():
                    for codigo in mapa_modif[regra.codigo_usina]:
                        self.aplica_regra_qdef_modif(
                            regra, modif, codigo=codigo
                        )
                else:
                    self.aplica_regra_qdef_modif(regra, modif)
                modif.escreve_arquivo(self._caso.caminho)
            # Aplica a restrição da defluência máxima, se houver,
            # no re.dat
            mapa_re = AplicadorRegrasReservatoriosNEWAVE.MAPA_FICTICIAS_RE
            if regra.limite_maximo is not None:
                re = RE.le_arquivo(self._caso.caminho)
                if regra.codigo_usina in mapa_re.keys():
                    for codigo in mapa_re[regra.codigo_usina]:
                        self.aplica_regra_qdef_re(regra, re, codigo=codigo)
                else:
                    self.aplica_regra_qdef_re(regra, re)
                re.escreve_arquivo(self._caso.caminho)
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
                if isinstance(c, CasoDECOMP) and c.mes == mes_anterior
            )
        except StopIteration:
            Log.log().info(
                f"Caso {self._caso.nome} não possui DECOMP anterior. "
                + "Não serão aplicadas regras operativas de reservatórios."
            )
            return True

        arq_relato = f"relato.rv{ultimo_decomp.revisao}"
        relato = Relato.le_arquivo(ultimo_decomp.caminho, arq_relato)

        # Filtra as regras de operação para o mês do caso
        regras_mes = self.regras_mes(regras_operacao, self._caso.mes)

        # Identifica as regras ativas
        regras_ativas = self.identifica_regras_ativas(regras_mes, relato)

        sucessos: List[bool] = []
        # Para o NEWAVE, são sempre tomadas as regras vigentes para os
        # volumes do últimos estágio semanal do último DECOMP do mês anterior
        estagio = sorted(list(regras_ativas.keys()))[-1]
        for r in regras_ativas[estagio]:
            sucessos.append(self.aplica_regra(r))
        return all(sucessos)


class AplicadorRegrasReservatoriosDECOMP(AplicadorRegrasReservatorios):
    def __init__(self, caso: CasoDECOMP) -> None:
        super().__init__(caso)

    # Override
    def identifica_regra_ativa(
        self,
        regras: List[RegraReservatorio],
        codigo_usina: int,
        volumes: pd.DataFrame,
        estagio: int,
    ) -> Optional[RegraReservatorio]:
        # TODO - Assume que uma usina só olha para um reservatório.
        # Não suporta o caso de uma usina depender de mais
        # de um volume.
        codigo_reservatorio = next(
            r.codigo_reservatorio
            for r in regras
            if r.codigo_usina == codigo_usina
        )
        volume_estagio = float(
            volumes.loc[
                volumes["Número"] == codigo_reservatorio,
                f"Estágio {estagio}",
            ]
        )
        try:
            regra = None
            for r in regras:
                limsup = 100.1 if r.volume_maximo == 100.0 else r.volume_maximo
                if all(
                    [
                        r.codigo_usina == codigo_usina,
                        r.codigo_reservatorio == codigo_reservatorio,
                        r.volume_minimo <= float(volume_estagio) < limsup,
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
                + f"(reservatório {codigo_reservatorio}) "
                + f"no volume {float(volume_estagio)}"
            )
            regra = None
        return regra

    def identifica_regras_ativas(
        self, regras: Dict[int, List[RegraReservatorio]], relato: Relato
    ) -> Dict[int, List[RegraReservatorio]]:
        # Obtém os volumes
        volumes = relato.volume_util_reservatorios

        regras_ativas_estagios: Dict[int, List[RegraReservatorio]] = {}
        # Obtém as regras ativas para cada usina
        for estagio, regras_estagio in regras.items():
            usinas_com_restricao = list(
                set([r.codigo_usina for r in regras_estagio])
            )
            regras_ativas: List[RegraReservatorio] = []
            for u in usinas_com_restricao:
                regra_estagio = self.identifica_regra_ativa(
                    regras[estagio], u, volumes, estagio
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
            try:
                cqs = dadger.lista_registros(CQ)
                cqs_usina = [
                    c for c in cqs if c._dados[2] == regra.codigo_usina
                ]
                if len(cqs_usina) > 0:
                    codigos_restricoes = [cq._dados[0] for cq in cqs_usina]
                else:
                    codigos_restricoes = [cqs[-1]._dados[0] + 1]
                    cqs_usinas = [CQ()]
                    cqs_usinas[0]._dados = [
                        codigos_restricoes[0],
                        1,
                        regra.codigo_usina,
                        1.0,
                        regra.tipo_restricao,
                    ]
                efs = [
                    dadger.hq(codigo).estagio_final
                    for codigo in codigos_restricoes
                ]
            except ValueError:
                for cq_usina, codigo, ef in zip(
                    cqs_usina, codigos_restricoes, efs
                ):
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
        ultimo_decomp: CasoDECOMP,
        gap_semanas: int = 0,
        regras_mensais: bool = False,
    ) -> bool:

        # Lê o dadger do decomp atual e o relato do decomp anterior
        arq_dadger = f"dadger.rv{self._caso.revisao}"
        dadger = Dadger.le_arquivo(self._caso.caminho, arq_dadger)
        arq_relato = f"relato.rv{ultimo_decomp.revisao}"
        relato = Relato.le_arquivo(ultimo_decomp.caminho, arq_relato)

        # Identifica o dia de fim de cada semana do DECOMP anterior
        mapa_dias_fim = self.mapeia_semanas_dias_fim(
            dadger, relato, gap_semanas
        )
        # Se está falando de regras mensais, não consulta semana a semana
        if regras_mensais:
            ultimo_estagio = list(mapa_dias_fim.keys())[-1]
            mapa_dias_fim = {1: mapa_dias_fim[ultimo_estagio]}
            pass
        Log.log().info(
            f"Dias de fim dos estágios do DECOMP anterior: {mapa_dias_fim}"
        )

        # Filtra as regras de operação para cada estágio
        # do DECOMP anterior
        regras_estagios = self.regras_estagios(regras_operacao, mapa_dias_fim)

        # Identifica as regras ativas
        regras_ativas = self.identifica_regras_ativas(regras_estagios, relato)

        # Aplica as regras ativas
        registros_dp = dadger.lista_registros(DP)
        num_subsistemas = len(dadger.lista_registros(SB))
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
                dadger = Dadger.le_arquivo(self._caso.caminho, arq_dadger)
                sucessos.append(self.aplica_regra(dadger, r, estagio))
                dadger.escreve_arquivo(self._caso.caminho, arq_dadger)

        return all(sucessos)

    def aplica_regras(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
        regras_semanais = list(
            set([r for r in regras_operacao if r.periodicidade == "S"])
        )
        try:
            ultimo_decomp = next(
                c
                for c in reversed(casos_anteriores)
                if isinstance(c, CasoDECOMP)
            )
            self.aplica_regras_caso(regras_semanais, ultimo_decomp)
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
                if isinstance(c, CasoDECOMP) and c.mes == mes_anterior
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
                regras_mensais, ultimo_decomp_mes_anterior, gap_semanas, True
            )
        except StopIteration:
            Log.log().info(
                f"Caso {self._caso.nome} não possui DECOMP no mês anterior. "
                + "Não serão aplicadas regras operativas de reservatórios "
                + "com periodicidade mensal."
            )

        return True
