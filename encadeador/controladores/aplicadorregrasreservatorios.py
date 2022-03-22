from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.utils.log import Log

from idecomp.decomp.modelos.dadger import DP
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.relato import Relato
from abc import abstractmethod
from typing import List, Dict, Optional
import pandas as pd  # type: ignore


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
                            <= r.volume_maximo,
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
        self,
        regras: List[RegraReservatorio],
        relato: Relato,
        estagio_mensal=False,
    ) -> Dict[str, List[RegraReservatorio]]:
        # Obtém os volumes
        volumes = relato.volume_util_reservatorios
        # Filtra as colunas de estágios de interesse
        cols_estagios = [c for c in volumes.columns if "Estágio" in c]
        cols_estagios = (
            [cols_estagios[-1]] if estagio_mensal else cols_estagios[:-1]
        )
        # Obtém as regras ativas para cada usina
        usinas_com_restricao = list(set([r.codigo_usina for r in regras]))
        regras_ativas: Dict[str, List[RegraReservatorio]] = {[]}
        for u in usinas_com_restricao:
            regras_estagios = self.identifica_regra_ativa(
                regras, u, volumes, cols_estagios
            )
            for c in cols_estagios:
                if regras_estagios[c] is not None:
                    regras_ativas[c].append(regras_estagios[c])
        return regras_ativas

    def aplica_regra(
        self,
        dadger: Dadger,
        regra: RegraReservatorio,
        estagio_decomp_anterior: int,
    ) -> bool:
        # Descobre o número de estágios do caso atual
        num_estagios_caso_anterior = self.extrai_numero_estagios(dadger)
        # Descobre em quais estágios aplicar a regra, no caso atual
        estagios_caso_atual = self.mapeia_estagios_regras(
            num_estagios_caso_anterior, estagio_decomp_anterior
        )
        for e in estagios_caso_atual:
            Log.log().info(f"Aplicando regra: {str(regra)} no estágio {e}")
            # Se ocorrer algum erro, retorna False
            pass
        return True

    def extrai_numero_estagios(self, dadger: Dadger) -> int:
        return len(dadger.lista_registros(DP))

    def mapeia_estagios_regras(
        self,
        num_estagios_caso_atual: int,
        estagio_caso_anterior: int,
    ) -> List[int]:
        if self._caso.revisao == 0:
            return {
                1: list(range(1, num_estagios_caso_atual)),
                2: [num_estagios_caso_atual],
            }
        else:
            return [estagio_caso_anterior]

    def aplica_regras(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
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
        # Filtra as regras de operação para o mês do caso
        regras_mes = self.regras_mes(regras_operacao, self._caso.mes)
        # Filtra as regras de operação para o mês seguinte
        regras_mes_seguinte = self.regras_mes(
            regras_operacao, (self._caso.mes % 12) + 1
        )
        # Lê o relato do último DECOMP
        arq_relato = f"relato.rv{ultimo_decomp.revisao}"
        relato = Relato.le_arquivo(ultimo_decomp.caminho, arq_relato)
        # Identifica as regras ativas
        regras_ativas_mes = self.identifica_regras_ativas(regras_mes, relato)
        regras_ativas_mes_seguinte = self.identifica_regras_ativas(
            regras_mes_seguinte, relato, estagio_mensal=True
        )
        regras_ativas = {**regras_ativas_mes, **regras_ativas_mes_seguinte}
        if len(regras_ativas) > 0:
            arq_dadger = f"dadger.rv{self._caso.revisao}"
            dadger = Dadger.le_arquivo(self._caso.caminho, arq_dadger)
        else:
            Log.log().warning(
                "Não foram encontradas regras ativas para "
                + f"os reservatórios no caso {self._caso.nome}."
            )
            return False
        # Aplica as regras ativas
        sucessos: List[bool] = []
        for estagio, regras in regras_ativas.items():
            for r in regras:
                sucessos.append(
                    self.aplica_regra(
                        dadger, r, int(estagio.split("Estágio")[1])
                    )
                )
        # Escreve o dadger
        dadger.escreve_arquivo(self._caso.caminho, arq_dadger)
        return all(sucessos)
