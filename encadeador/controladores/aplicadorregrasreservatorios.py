from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.utils.log import Log

from inewave.newave.modelos.modif import USINA, VAZMINT
from inewave.newave import Modif, RE
from idecomp.decomp.modelos.dadger import DP, HQ, LQ
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.relato import Relato
from idecomp.decomp.hidr import Hidr
from abc import abstractmethod
from typing import List, Dict, Optional
import pandas as pd  # type: ignore
from datetime import date, timedelta
from dateutil.relativedelta import relativedelta


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
    def identifica_regra_ativa(
        self,
        regras: List[RegraReservatorio],
        codigo_usina: int,
        volumes: pd.DataFrame,
    ) -> Optional[RegraReservatorio]:
        pass

    @abstractmethod
    def identifica_regras_ativas(
        self, regras: List[RegraReservatorio], ultimo_decomp: CasoDECOMP
    ):
        pass

    @abstractmethod
    def aplica_regra(self, regra: RegraReservatorio):
        pass

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

    # Override
    def identifica_regra_ativa(
        self,
        regras: List[RegraReservatorio],
        codigo_usina: int,
        volumes: pd.DataFrame,
        estagio: int,
    ) -> Dict[int, Optional[RegraReservatorio]]:
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
        regras_ativas: Dict[str, Optional[RegraReservatorio]] = {}
        try:
            regra = next(
                r
                for r in regras
                if all(
                    [
                        r.codigo_usina == codigo_usina,
                        r.codigo_reservatorio == codigo_reservatorio,
                        r.volume_minimo
                        <= float(volume_estagio)
                        < r.volume_maximo,
                    ]
                )
            )
        except StopIteration:
            Log.log().warning(
                "Não foi encontrada regra de operação ativa "
                + f"para a usina {codigo_usina} "
                + f"(reservatório {codigo_reservatorio}) "
                + f"no volume {float(volume_estagio)}"
            )
            regra = None
            regras_ativas[estagio] = regra
        return regras_ativas

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
            regras_ativas: Dict[str, List[RegraReservatorio]] = {[]}
            for u in usinas_com_restricao:
                regras_estagios = self.identifica_regra_ativa(
                    regras, u, volumes, estagio
                )
                if regras_estagios is not None:
                    regras_ativas[estagio].append(regras_estagios)
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
        # Se a usina em questão não é modificada, cria uma modificação nova
        if not any([m.codigo == codigo for m in modif.usina]):
            pass
        # Obtém o registro que modifica a usina
        usina = next(m for m in modif.usina if m.codigo == codigo)
        # Obtém o próximo registro de usina
        idx_usina = usina._ordem
        idx_proxima_usina = modif.usina[modif.usina.index(usina) + 1]._ordem
        # TODO - REVER PREMISSA
        # Se existem VAZMINT para a usina, deleta
        vazmint_existentes = [
            m
            for m in modif.vazmint
            if idx_usina <= m._ordem <= idx_proxima_usina
        ]
        # Guarda a vazão do último VAZMINT
        if len(vazmint_existentes) > 0:
            ultima_vazao = vazmint_existentes[-1].vazao
        else:
            hidr = Hidr.le_arquivo(self._caso.caminho).tabela
            ultima_vazao = float(hidr.loc[codigo, "Vazão Mínima"])
        for m in vazmint_existentes:
            modif.deleta_registro(m)
        # Cria os VAZMINT
        # - O primeiro é válido para os 2 primeiros meses
        novo_vazmint = VAZMINT()
        novo_vazmint.mes = self._caso.mes
        novo_vazmint.ano = self._caso.ano
        novo_vazmint.vazao = regra.limite_minimo
        modif.cria_registro(usina, novo_vazmint)
        # - O segundo é para retornar ao valor anterior
        fim_vazmint = date(
            year=self._caso.ano, month=self._caso.mes, day=1
        ) + relativedelta(months=+2)
        prox_vazmint = VAZMINT()
        prox_vazmint.mes = fim_vazmint.month
        prox_vazmint.ano = fim_vazmint.year
        prox_vazmint.vazao = ultima_vazao
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
        if not any(
            [
                codigo
                in df_conjuntos.loc[
                    df_conjuntos["Conjunto"] == c, cols_usinas
                ].tolist()
                for c in conjuntos
            ]
        ):
            num_conjunto = max(conjuntos) + 1
            novo_conjunto = {
                **{"Conjunto": [num_conjunto]},
                **{c: [0] for c in cols_usinas},
            }
            novo_conjunto["Usina 1"] = [codigo]
            re.usinas_conjuntos = df_conjuntos.append(
                pd.DataFrame(data=novo_conjunto), ignore_index=True
            )
        # Senão, identifica.
        num_conjunto = next(
            c
            for c in conjuntos
            if codigo
            in df_conjuntos.loc[
                df_conjuntos["Conjunto"] == c, cols_usinas
            ].tolist()
        )
        # Deleta as restrições do conjunto em questão, se existirem
        restricoes = re.restricoes
        indices_restricoes = restricoes.loc[
            restricoes["Conjunto"] == num_conjunto, :
        ].index
        restricoes = restricoes.drop(index=indices_restricoes)
        # Cria as restrições para o conjunto em questão, nos 2 primeiros
        # meses do horizonte
        mes_inicial = date(year=self._caso.ano, month=self._caso.mes, day=1)
        mes_final = mes_inicial + relativedelta(months=+1)
        nova_restricao = {
            "Conjunto": [num_conjunto],
            "Mês Início": [mes_inicial.month],
            "Ano Início": [mes_inicial.year],
            "Mês Fim": [mes_final.month],
            "Ano Fim": [mes_final.year],
            "Flag P": [0],
            "Restrição": [self.obtem_ghmax_usina(codigo, regra.limite_maximo)],
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
                + f"Não serão aplicadas regras operativas de reservatórios."
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
            sucessos.append(self.aplica_regra(r, estagio))
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
    ) -> Dict[int, Optional[RegraReservatorio]]:
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
        regras_ativas: Dict[str, Optional[RegraReservatorio]] = {}
        try:
            regra = next(
                r
                for r in regras
                if all(
                    [
                        r.codigo_usina == codigo_usina,
                        r.codigo_reservatorio == codigo_reservatorio,
                        r.volume_minimo
                        <= float(volume_estagio)
                        < r.volume_maximo,
                    ]
                )
            )
        except StopIteration:
            Log.log().warning(
                "Não foi encontrada regra de operação ativa "
                + f"para a usina {codigo_usina} "
                + f"(reservatório {codigo_reservatorio}) "
                + f"no volume {float(volume_estagio)}"
            )
            regra = None
            regras_ativas[estagio] = regra
        return regras_ativas

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
            regras_ativas: Dict[str, List[RegraReservatorio]] = {[]}
            for u in usinas_com_restricao:
                regras_estagios = self.identifica_regra_ativa(
                    regras, u, volumes, estagio
                )
                if regras_estagios is not None:
                    regras_ativas[estagio].append(regras_estagios)
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
                ef = dadger.hq(regra.codigo_usina).estagio_final
            except ValueError:
                # Se não existe o registro HQ, cria, junto com um LQ
                ef = len(dadger.lista_registros(DP))
                hq_novo = HQ()
                hq_novo._dados = [regra.codigo_usina, 1, ef]
                lq_novo = LQ()
                lq_novo._dados = [regra.codigo_usina, 1] + [
                    0,
                    99999,
                    0,
                    99999,
                    0,
                    99999,
                ]
                dadger.cria_registro(dadger.ev, hq_novo)
                dadger.cria_registro(hq_novo, lq_novo)
            for e in range(estagio, ef + 1):
                dadger.lq(regra.codigo_usina, e)
            # Aplica a regra no estágio devido, se tiver limites inf/sup
            if regra.limite_minimo is not None:
                dadger.lq(regra.codigo_usina, estagio).limites_inferiores = [
                    regra.limite_minimo
                ] * 3
            if regra.limite_maximo is not None:
                dadger.lq(regra.codigo_usina, estagio).limites_superiores = [
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
        self, dadger: Dadger, relato: Relato
    ) -> Dict[int, date]:
        dt = dadger.dt
        dia_inicio_caso_atual = date(dt.ano, dt.mes, dt.dia)
        num_semanas_caso_anterior = (
            len(relato.volume_util_reservatorios.columns) - 2
        )
        return {
            i
            + 1: dia_inicio_caso_atual
            - timedelta(weeks=(num_semanas_caso_anterior - i), days=1)
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
    ) -> bool:

        # Lê o dadger do decomp atual e o relato do decomp anterior
        arq_dadger = f"dadger.rv{self._caso.revisao}"
        dadger = Dadger.le_arquivo(self._caso.caminho, arq_dadger)
        arq_relato = f"relato.rv{ultimo_decomp.revisao}"
        relato = Relato.le_arquivo(ultimo_decomp.caminho, arq_relato)

        # Identifica o dia de fim de cada semana do DECOMP anterior
        mapa_dias_fim = self.mapeia_semanas_dias_fim(dadger, relato)

        # Filtra as regras de operação para cada estágio
        # do DECOMP anterior
        regras_estagios = self.regras_estagios(regras_operacao, mapa_dias_fim)

        # Identifica as regras ativas
        regras_ativas = self.identifica_regras_ativas(regras_estagios, relato)

        # Aplica as regras ativas
        estagios_decomp_atual = list(
            range(len(dadger.lista_registros(DP)) + 1, start=1)
        )
        sucessos: List[bool] = []
        for estagio in estagios_decomp_atual:
            if estagio not in regras_ativas.keys():
                estagio_aplicacao = sorted(list(regras_ativas.keys()))[-1]
            else:
                estagio_aplicacao = estagio
            for r in regras_ativas[estagio_aplicacao]:
                sucessos.append(
                    self.aplica_regra(dadger, r, estagio_aplicacao)
                )
        # Escreve o dadger
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
                + f"Não serão aplicadas regras operativas de reservatórios "
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
            self.aplica_regras_caso(regras_mensais, ultimo_decomp_mes_anterior)
        except StopIteration:
            Log.log().info(
                f"Caso {self._caso.nome} não possui DECOMP no mês anterior. "
                + f"Não serão aplicadas regras operativas de reservatórios "
                + "com periodicidade mensal."
            )
