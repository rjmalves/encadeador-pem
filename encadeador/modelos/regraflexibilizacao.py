from abc import abstractmethod
from logging import Logger
from typing import List, Tuple
from idecomp.decomp.dadger import Dadger  # type: ignore

from encadeador.modelos.inviabilidade import Inviabilidade
from encadeador.modelos.inviabilidade import InviabilidadeEV
from encadeador.modelos.inviabilidade import InviabilidadeTI
from encadeador.modelos.inviabilidade import InviabilidadeHV
from encadeador.modelos.inviabilidade import InviabilidadeHQ
from encadeador.modelos.inviabilidade import InviabilidadeRE
from encadeador.modelos.inviabilidade import InviabilidadeHE
from encadeador.modelos.inviabilidade import InviabilidadeDeficit


class RegraFlexibilizacao:

    tipos_inviabilidades = [
                            InviabilidadeEV,
                            InviabilidadeTI,
                            InviabilidadeHV,
                            InviabilidadeHQ,
                            InviabilidadeRE,
                            InviabilidadeHE,
                            InviabilidadeDeficit
                           ]

    deltas_inviabilidades = {
                             InviabilidadeEV: 0,
                             InviabilidadeTI: 0.2,
                             InviabilidadeHV: 1,
                             InviabilidadeHQ: 5,
                             InviabilidadeRE: 1,
                             InviabilidadeHE: 0.1,
                             InviabilidadeDeficit: 0
                            }

    def __init__(self,
                 log: Logger) -> None:
        self._log = log

    @staticmethod
    def factory(metodo: str,
                log: Logger) -> 'RegraFlexibilizacao':
        if metodo == "absoluto":
            return RegraFlexibilizacaoAbsoluto(log)
        else:
            raise ValueError(f"Regra de flexibilização {metodo} não suportada")

    @abstractmethod
    def _flexibilizaEV(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeEV]):
        pass

    @abstractmethod
    def _flexibilizaTI(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeTI]):
        pass

    @abstractmethod
    def _flexibilizaHV(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeHV]):
        pass

    @abstractmethod
    def _flexibilizaHQ(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeHQ]):
        pass

    @abstractmethod
    def _flexibilizaRE(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeRE]):
        pass

    @abstractmethod
    def _flexibilizaHE(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeHE]):
        pass

    @abstractmethod
    def _flexibiliza_deficit(self,
                             dadger: Dadger,
                             inviabilidades: List[InviabilidadeDeficit]):
        pass

    def flexibiliza(self,
                    dadger: Dadger,
                    inviabilidades: List[Inviabilidade]):

        # Agrupa as inviabilidades por tipo
        tipos = RegraFlexibilizacao.tipos_inviabilidades
        invs_por_tipo: dict = {t: [] for t in tipos}
        for inv in inviabilidades:
            invs_por_tipo[type(inv)].append(inv)

        # Flexibiliza cada tipo
        self._flexibilizaEV(dadger, invs_por_tipo[InviabilidadeEV])
        self._flexibilizaTI(dadger, invs_por_tipo[InviabilidadeTI])
        self._flexibilizaHV(dadger, invs_por_tipo[InviabilidadeHV])
        self._flexibilizaHQ(dadger, invs_por_tipo[InviabilidadeHQ])
        self._flexibilizaRE(dadger, invs_por_tipo[InviabilidadeRE])
        self._flexibilizaHE(dadger, invs_por_tipo[InviabilidadeHE])
        self._flexibiliza_deficit(dadger, invs_por_tipo[InviabilidadeDeficit])


class RegraFlexibilizacaoAbsoluto(RegraFlexibilizacao):

    def __init__(self,
                 log: Logger) -> None:
        super().__init__(log)

    # Override
    def _flexibilizaEV(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeEV]):

        def __identifica_inv(inv: InviabilidadeEV) -> Tuple[int, int]:
            return (inv._codigo, inv._estagio)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeEV],
                                               inv_ini: InviabilidadeEV
                                               ) -> InviabilidadeEV:
            max_viol = inv_ini
            ident_ini = __identifica_inv(inv_ini)
            invs_mesma_id = [i for i in invs if
                             __identifica_inv(i) == ident_ini]
            for i in invs_mesma_id:
                if i._violacao > max_viol._violacao:
                    max_viol = i
            return max_viol

        # Estrutura para conter os pares (código, estágio) já flexibilizados
        flexibilizados: List[Tuple[int, int]] = []
        for inv in inviabilidades:
            identificacao = __identifica_inv(inv)
            # Se já flexibilizou essa restrição nesse estágio, ignora
            if identificacao in flexibilizados:
                continue
            # Senão, procura dentre todas as outras pela maior violação
            flexibilizados.append(identificacao)
            _ = __inv_maxima_violacao_identificada(inviabilidades, inv)
            # Flexibiliza (TODO)

    # Override
    def _flexibilizaTI(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeTI]):

        def __identifica_inv(inv: InviabilidadeTI) -> Tuple[int, int]:
            return (inv._codigo, inv._estagio)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeTI],
                                               inv_ini: InviabilidadeTI
                                               ) -> InviabilidadeTI:
            max_viol = inv_ini
            ident_ini = __identifica_inv(inv_ini)
            invs_mesma_id = [i for i in invs if
                             __identifica_inv(i) == ident_ini]
            for i in invs_mesma_id:
                if i._violacao > max_viol._violacao:
                    max_viol = i
            return max_viol

        # Estrutura para conter os pares (código, estágio) já flexibilizados
        flexibilizados: List[Tuple[int, int]] = []
        for inv in inviabilidades:
            identificacao = __identifica_inv(inv)
            # Se já flexibilizou essa restrição nesse estágio, ignora
            if identificacao in flexibilizados:
                continue
            # Senão, procura dentre todas as outras pela maior violação
            flexibilizados.append(identificacao)
            max_viol = __inv_maxima_violacao_identificada(inviabilidades,
                                                          inv)
            # Flexibiliza
            idx = max_viol._estagio - 1
            reg = dadger.ti(max_viol._codigo)
            valor_atual = reg.taxas[idx]
            deltas = RegraFlexibilizacao.deltas_inviabilidades
            novo_valor = max([0, valor_atual - deltas[InviabilidadeTI]])
            dadger.ti(max_viol._codigo).taxas[idx] = novo_valor
            self._log.info(f"Flexibilizando TI {max_viol._codigo} -" +
                           f" Estágio {max_viol._estagio}: " +
                           f"{valor_atual} -> {novo_valor}")

    # Override
    def _flexibilizaHV(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeHV]):

        def __identifica_inv(inv: InviabilidadeHV) -> Tuple[int, int, str]:
            return (inv._codigo, inv._estagio, inv._limite)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeHV],
                                               inv_ini: InviabilidadeHV
                                               ) -> InviabilidadeHV:
            max_viol = inv_ini
            ident_ini = __identifica_inv(inv_ini)
            invs_mesma_id = [i for i in invs if
                             __identifica_inv(i) == ident_ini]
            for i in invs_mesma_id:
                if i._violacao > max_viol._violacao:
                    max_viol = i
            return max_viol

        # Estrutura para conter as tuplas
        # (código, estágio, limite) já flexibilizados
        flexibilizados: List[Tuple[int, int, str]] = []
        for inv in inviabilidades:
            identificacao = __identifica_inv(inv)
            # Se já flexibilizou essa restrição nesse estágio, ignora
            if identificacao in flexibilizados:
                continue
            # Senão, procura dentre todas as outras pela maior violação
            flexibilizados.append(identificacao)
            max_viol = __inv_maxima_violacao_identificada(inviabilidades,
                                                          inv)
            # Flexibiliza
            reg = dadger.lv(max_viol._codigo, max_viol._estagio)
            deltas = RegraFlexibilizacao.deltas_inviabilidades
            if max_viol._limite == "L. INF":
                valor_atual = reg.limite_inferior
                valor_flex = max_viol._violacao + deltas[InviabilidadeHV]
                novo_valor = max([0, valor_atual - valor_flex])
                dadger.lv(max_viol._codigo,
                          max_viol._estagio).limite_inferior = novo_valor
            elif max_viol._limite == "L. SUP":
                valor_atual = reg.limites_superior
                valor_flex = max_viol._violacao + deltas[InviabilidadeHV]
                novo_valor = min([99999, valor_atual + valor_flex])
                dadger.lv(max_viol._codigo,
                          max_viol._estagio).limite_inferior = novo_valor
            self._log.info(f"Flexibilizando HV {max_viol._codigo} - Estágio" +
                           f" {max_viol._estagio} - {max_viol._limite}: " +
                           f"{valor_atual} -> {novo_valor}")

    # Override
    def _flexibilizaHQ(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeHQ]):

        def __identifica_inv(inv: InviabilidadeHQ) -> Tuple[int,
                                                            int,
                                                            str,
                                                            int]:
            return (inv._codigo, inv._estagio, inv._limite, inv._patamar)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeHQ],
                                               inv_ini: InviabilidadeHQ
                                               ) -> InviabilidadeHQ:
            max_viol = inv_ini
            ident_ini = __identifica_inv(inv_ini)
            invs_mesma_id = [i for i in invs if
                             __identifica_inv(i) == ident_ini]
            for i in invs_mesma_id:
                if i._violacao > max_viol._violacao:
                    max_viol = i
            return max_viol

        # Estrutura para conter as tuplas
        # (código, estágio, limite, patamar) já flexibilizados
        flexibilizados: List[Tuple[int, int, str, int]] = []
        for inv in inviabilidades:
            identificacao = __identifica_inv(inv)
            # Se já flexibilizou essa restrição nesse estágio, ignora
            if identificacao in flexibilizados:
                continue
            # Senão, procura dentre todas as outras pela maior violação
            flexibilizados.append(identificacao)
            max_viol = __inv_maxima_violacao_identificada(inviabilidades,
                                                          inv)
            # Flexibiliza
            reg = dadger.lq(max_viol._codigo, max_viol._estagio)
            deltas = RegraFlexibilizacao.deltas_inviabilidades
            idx = max_viol._patamar - 1
            if max_viol._limite == "L. INF":
                valor_atual = reg.limites_inferiores[idx]
                valor_flex = max_viol._violacao + deltas[InviabilidadeHQ]
                novo = max([0, valor_atual - valor_flex])
                dadger.lq(max_viol._codigo,
                          max_viol._estagio).limites_inferiores[idx] = novo
            elif max_viol._limite == "L. SUP":
                valor_atual = reg.limites_superiores[idx]
                valor_flex = max_viol._violacao + deltas[InviabilidadeHQ]
                novo = min([99999, valor_atual + valor_flex])
                dadger.lq(max_viol._codigo,
                          max_viol._estagio).limites_superiores[idx] = novo
            self._log.info(f"Flexibilizando HQ {max_viol._codigo} - Estágio" +
                           f" {max_viol._estagio} pat {max_viol._patamar}" +
                           f" - {max_viol._limite}: " +
                           f"{valor_atual} -> {novo}")

    # Override
    def _flexibilizaRE(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeRE]):

        def __identifica_inv(inv: InviabilidadeRE) -> Tuple[int,
                                                            int,
                                                            str,
                                                            int]:
            return (inv._codigo, inv._estagio, inv._limite, inv._patamar)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeRE],
                                               inv_ini: InviabilidadeRE
                                               ) -> InviabilidadeRE:
            max_viol = inv_ini
            ident_ini = __identifica_inv(inv_ini)
            invs_mesma_id = [i for i in invs if
                             __identifica_inv(i) == ident_ini]
            for i in invs_mesma_id:
                if i._violacao > max_viol._violacao:
                    max_viol = i
            return max_viol

        # Estrutura para conter as tuplas
        # (código, estágio, limite, patamar) já flexibilizados
        flexibilizados: List[Tuple[int, int, str, int]] = []
        for inv in inviabilidades:
            identificacao = __identifica_inv(inv)
            # Se já flexibilizou essa restrição nesse estágio, ignora
            if identificacao in flexibilizados:
                continue
            # Senão, procura dentre todas as outras pela maior violação
            flexibilizados.append(identificacao)
            max_viol = __inv_maxima_violacao_identificada(inviabilidades,
                                                          inv)
            # Flexibiliza
            reg = dadger.lu(max_viol._codigo, max_viol._estagio)
            deltas = RegraFlexibilizacao.deltas_inviabilidades
            idx = max_viol._patamar - 1
            if max_viol._limite == "L. INF":
                valor_atual = reg.limites_inferiores[idx]
                valor_flex = max_viol._violacao + deltas[InviabilidadeRE]
                novo = max([0, valor_atual - valor_flex])
                dadger.lu(max_viol._codigo,
                          max_viol._estagio).limites_inferiores[idx] = novo
            elif max_viol._limite == "L. SUP":
                valor_atual = reg.limites_superiores[idx]
                valor_flex = max_viol._violacao + deltas[InviabilidadeRE]
                novo = min([99999, valor_atual + valor_flex])
                dadger.lu(max_viol._codigo,
                          max_viol._estagio).limites_superiores[idx] = novo
            self._log.info(f"Flexibilizando RE {max_viol._codigo} - Estágio" +
                           f" {max_viol._estagio} pat {max_viol._patamar}" +
                           f" - {max_viol._limite}: " +
                           f"{valor_atual} -> {novo}")

    # Override
    def _flexibilizaHE(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeHE]):

        def __identifica_inv(inv: InviabilidadeHE) -> Tuple[int, int, str]:
            return (inv._codigo, inv._estagio, inv._limite)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeHE],
                                               inv_ini: InviabilidadeHE
                                               ) -> InviabilidadeHE:
            max_viol = inv_ini
            ident_ini = __identifica_inv(inv_ini)
            invs_mesma_id = [i for i in invs if
                             __identifica_inv(i) == ident_ini]
            for i in invs_mesma_id:
                if i._violacao > max_viol._violacao:
                    max_viol = i
            return max_viol

        # Estrutura para conter as tuplas
        # (código, estágio, limite) já flexibilizados
        flexibilizados: List[Tuple[int, int, str]] = []
        for inv in inviabilidades:
            identificacao = __identifica_inv(inv)
            # Se já flexibilizou essa restrição nesse estágio, ignora
            if identificacao in flexibilizados:
                continue
            # Senão, procura dentre todas as outras pela maior violação
            flexibilizados.append(identificacao)
            max_viol = __inv_maxima_violacao_identificada(inviabilidades,
                                                          inv)
            # Flexibiliza
            reg = dadger.he(max_viol._codigo, max_viol._estagio)
            deltas = RegraFlexibilizacao.deltas_inviabilidades
            if max_viol._limite != "L. INF":
                raise RuntimeError("Restrições RHE só aceitas para L. INF")
            if max_viol._unidade == "%":
                delta = deltas[InviabilidadeHE]
            if max_viol._unidade == "MWmes":
                delta = 100 * deltas[InviabilidadeHE]
            valor_atual = reg.limite
            valor_flex = max_viol._violacao + delta
            novo_valor = max([0, valor_atual - valor_flex])
            dadger.he(max_viol._codigo, max_viol._estagio).limite = novo_valor
            self._log.info(f"Flexibilizando HE {max_viol._codigo} - Estágio" +
                           f" {max_viol._estagio} - {max_viol._limite}: " +
                           f"{valor_atual} -> {novo_valor}")

    # Override
    def _flexibiliza_deficit(self,
                             dadger: Dadger,
                             inviabilidades: List[InviabilidadeDeficit]):
        if len(inviabilidades) > 0:
            raise RuntimeError("Flex. de déficit não implementada")
