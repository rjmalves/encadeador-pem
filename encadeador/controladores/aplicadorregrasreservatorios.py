from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.utils.log import Log

from idecomp.decomp.modelos.dadger import DP, HQ, LQ
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.relato import Relato
from abc import abstractmethod
from typing import List, Dict, Optional
import pandas as pd  # type: ignore
from datetime import date, timedelta


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

    # Override
    def identifica_regra_ativa(
        self,
        regras: List[RegraReservatorio],
        codigo_usina: int,
        volumes: pd.DataFrame,
    ) -> Optional[RegraReservatorio]:
        # TODO - Assume que uma usina só olha para um reservatório.
        # Não suporta o caso de uma usina depender de mais
        # de um volume.
        codigo_reservatorio = next(
            r.codigo_reservatorio
            for r in regras
            if r.codigo_usina == codigo_usina
        )
        volume_inicio_mes = float(
            volumes.loc[volumes["Número"] == codigo_reservatorio, "Estágio 1"]
        )
        try:
            regra = next(
                r
                for r in regras
                if all(
                    [
                        r.codigo_usina == codigo_usina,
                        r.codigo_reservatorio == codigo_reservatorio,
                        r.volume_minimo
                        <= volume_inicio_mes
                        <= r.volume_maximo,
                    ]
                )
            )
        except StopIteration:
            Log.log().warning(
                "Não foi encontrada regra de operação ativa "
                + f"para a usina {codigo_usina} "
                + f"(reservatório {codigo_reservatorio}) "
                + f"no volume {volume_inicio_mes}"
            )
            regra = None
        return regra

    def identifica_regras_ativas(
        self, regras: List[RegraReservatorio], ultimo_decomp: CasoDECOMP
    ) -> List[RegraReservatorio]:
        # Lê o relato do último DECOMP
        arq_relato = f"relato.rv{ultimo_decomp.revisao}"
        relato = Relato.le_arquivo(ultimo_decomp.caminho, arq_relato)
        # Obtém os volumes
        volumes = relato.volume_util_reservatorios[
            ["Usina", "Número", "Estágio 1"]
        ]
        # Obtém as regras ativas para cada usina
        usinas_com_restricao = list(set([r.codigo_usina for r in regras]))
        regras_ativas: List[RegraReservatorio] = []
        for u in usinas_com_restricao:
            regra = self.identifica_regra_ativa(regras, u, volumes)
            if regra is not None:
                regras_ativas.append(regra)
        return regras_ativas

    def obtem_produtibilidade_usina(self, codigo: int) -> float:
        pass

    def aplica_regra(self, regra: RegraReservatorio) -> bool:
        # Extrai o valor esperado para o início do mês (aproximado na semana)
        # Obtem a produtibilidade para o cálculo do GHmax
        # Se ocorrer algum erro, retorna False
        pass

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
        # Filtra as regras de operação para o mês do caso
        regras_mes = self.regras_mes(regras_operacao, self._caso.mes)
        # Identifica as regras ativas
        regras_ativas = self.identifica_regras_ativas(
            regras_mes, ultimo_decomp
        )
        # Aplica as regras ativas
        sucessos: List[bool] = []
        for r in regras_ativas:
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
        estagios: List[str],
    ) -> Dict[str, Optional[RegraReservatorio]]:
        # TODO - Assume que uma usina só olha para um reservatório.
        # Não suporta o caso de uma usina depender de mais
        # de um volume.
        codigo_reservatorio = next(
            r.codigo_reservatorio
            for r in regras
            if r.codigo_usina == codigo_usina
        )
        volumes_estagios = volumes.loc[
            volumes["Número"] == codigo_reservatorio,
            estagios,
        ]
        regras_ativas: Dict[str, Optional[RegraReservatorio]] = {}
        for c in estagios:
            try:
                regra = next(
                    r
                    for r in regras
                    if all(
                        [
                            r.codigo_usina == codigo_usina,
                            r.codigo_reservatorio == codigo_reservatorio,
                            r.volume_minimo
                            <= float(volumes_estagios[c])
                            < r.volume_maximo,
                        ]
                    )
                )
            except StopIteration:
                Log.log().warning(
                    "Não foi encontrada regra de operação ativa "
                    + f"para a usina {codigo_usina} "
                    + f"(reservatório {codigo_reservatorio}) "
                    + f"no volume {float(volumes_estagios[c])}"
                )
                regra = None
            estagio = int(c.split("Estágio")[1])
            regras_ativas[estagio] = regra
        return regras_ativas

    def identifica_regras_ativas(
        self, regras: Dict[int, List[RegraReservatorio]], relato: Relato
    ) -> Dict[int, List[RegraReservatorio]]:
        # Obtém os volumes
        volumes = relato.volume_util_reservatorios
        # Filtra as colunas de estágios de interesse
        cols_estagios = [c for c in volumes.columns if "Estágio" in c]

        regras_ativas_estagios: Dict[int, List[RegraReservatorio]] = {}
        # Obtém as regras ativas para cada usina
        for estagio, regras_estagio in regras.items():
            usinas_com_restricao = list(
                set([r.codigo_usina for r in regras_estagio])
            )
            regras_ativas: Dict[str, List[RegraReservatorio]] = {[]}
            for u in usinas_com_restricao:
                regras_estagios = self.identifica_regra_ativa(
                    regras, u, volumes, cols_estagios
                )
                for c in cols_estagios:
                    if regras_estagios[c] is not None:
                        regras_ativas[c].append(regras_estagios[c])
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

    def aplica_regras(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:

        # TODO - tratar as regras mensais x semanais
        # Como está, todas são semanais.

        # Obtém o último DECOMP executado
        try:
            ultimo_decomp = next(
                c
                for c in reversed(casos_anteriores)
                if isinstance(c, CasoDECOMP)
            )
        except StopIteration:
            Log.log().info(
                f"Caso {self._caso.nome} não possui DECOMP anterior. "
                + f"Não serão aplicadas regras operativas de reservatórios."
            )
            return True

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
