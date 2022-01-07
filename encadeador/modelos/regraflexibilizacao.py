from abc import abstractmethod
from logging import Logger
from typing import List, Tuple
import numpy as np  # type: ignore
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.modelos.dadger import AC, ACVAZMIN, ACVERTJU, DP, FP, HE, CM

from encadeador.modelos.inviabilidade import Inviabilidade
from encadeador.modelos.inviabilidade import InviabilidadeEV
from encadeador.modelos.inviabilidade import InviabilidadeTI
from encadeador.modelos.inviabilidade import InviabilidadeHV
from encadeador.modelos.inviabilidade import InviabilidadeHQ
from encadeador.modelos.inviabilidade import InviabilidadeRE
from encadeador.modelos.inviabilidade import InviabilidadeHE
from encadeador.modelos.inviabilidade import InviabilidadeDEFMIN
from encadeador.modelos.inviabilidade import InviabilidadeFP
from encadeador.modelos.inviabilidade import InviabilidadeDeficit


class RegraFlexibilizacao:

    tipos_inviabilidades = [
                            InviabilidadeEV,
                            InviabilidadeTI,
                            InviabilidadeHV,
                            InviabilidadeHQ,
                            InviabilidadeRE,
                            InviabilidadeHE,
                            InviabilidadeDEFMIN,
                            InviabilidadeFP,
                            InviabilidadeDeficit
                           ]

    deltas_inviabilidades = {
                             InviabilidadeEV: 0,
                             InviabilidadeTI: 0.2,
                             InviabilidadeHV: 1,
                             InviabilidadeHQ: 5,
                             InviabilidadeRE: 1,
                             InviabilidadeHE: 0.1,
                             InviabilidadeDEFMIN: 0.2,
                             InviabilidadeFP: 0,
                             InviabilidadeDeficit: 2.0
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
    def _flexibilizaDEFMIN(self,
                           dadger: Dadger,
                           inviabilidades: List[InviabilidadeDEFMIN]):
        pass

    @abstractmethod
    def _flexibilizaFP(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeFP]):
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
        self._flexibilizaDEFMIN(dadger, invs_por_tipo[InviabilidadeDEFMIN])
        self._flexibilizaFP(dadger, invs_por_tipo[InviabilidadeFP])
        # PREMISSA
        # Só flexibiliza déficit se todas as inviabilidades forem déficit
        if len(inviabilidades) == len(invs_por_tipo[InviabilidadeDeficit]):
            self._flexibiliza_deficit(dadger,
                                      invs_por_tipo[InviabilidadeDeficit])


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
            max_viol = __inv_maxima_violacao_identificada(inviabilidades, inv)
            # Flexibiliza - Remove a consideração de evaporação na usina
            codigo = max_viol._codigo
            dadger.uh(codigo).evaporacao = False
            self._log.info(f"Flexibilizando EV {max_viol._codigo} " +
                           f" ({max_viol._nome_usina}) - " +
                           "Evaporação do registro UH desabilitada.")

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
            valor_flex = max_viol._violacao + deltas[InviabilidadeTI]
            novo_valor = max([0, valor_atual - valor_flex])
            novas_taxas = reg.taxas
            novas_taxas[idx] = novo_valor
            dadger.ti(max_viol._codigo).taxas = novas_taxas
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
                          max_viol._estagio).limites_superior = novo_valor
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
                novo_valor = max([0, valor_atual - valor_flex])
                novos = reg.limites_inferiores
                novos[idx] = novo_valor
                dadger.lq(max_viol._codigo,
                          max_viol._estagio).limites_inferiores = novos
            elif max_viol._limite == "L. SUP":
                valor_atual = reg.limites_superiores[idx]
                valor_flex = max_viol._violacao + deltas[InviabilidadeHQ]
                novo_valor = min([99999, valor_atual + valor_flex])
                novos = reg.limites_superiores
                novos[idx] = novo_valor
                dadger.lq(max_viol._codigo,
                          max_viol._estagio).limites_superiores = novos
            self._log.info(f"Flexibilizando HQ {max_viol._codigo} - Estágio" +
                           f" {max_viol._estagio} pat {max_viol._patamar}" +
                           f" - {max_viol._limite}: " +
                           f"{valor_atual} -> {novo_valor}")

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
                novo_valor = max([0, valor_atual - valor_flex])
                novos = reg.limites_inferiores
                novos[idx] = novo_valor
                dadger.lu(max_viol._codigo,
                          max_viol._estagio).limites_inferiores = novos
            elif max_viol._limite == "L. SUP":
                valor_atual = reg.limites_superiores[idx]
                valor_flex = max_viol._violacao + deltas[InviabilidadeRE]
                novo_valor = min([99999, valor_atual + valor_flex])
                novos = reg.limites_superiores
                novos[idx] = novo_valor
                dadger.lu(max_viol._codigo,
                          max_viol._estagio).limites_superiores = novos
            self._log.info(f"Flexibilizando RE {max_viol._codigo} - Estágio" +
                           f" {max_viol._estagio} pat {max_viol._patamar}" +
                           f" - {max_viol._limite}: " +
                           f"{valor_atual} -> {novo_valor}")

    # Override
    def _flexibilizaFP(self,
                       dadger: Dadger,
                       inviabilidades: List[InviabilidadeFP]):

        def __identifica_inv(inv: InviabilidadeFP) -> Tuple[int, int]:
            return (inv._codigo, inv._estagio)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeFP],
                                               inv_ini: InviabilidadeFP
                                               ) -> InviabilidadeFP:
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
            # Procura por um registro FP
            try:
                reg = dadger.fp(max_viol._codigo, 1)
                # Procura por um AC VERTJU
                try:
                    reg = dadger.ac(max_viol._codigo, "VERTJU")
                    self._log.info("Flexibilizando FP - " +
                                   "Registro AC VERTJU" +
                                   f" para a usina {max_viol._codigo} " +
                                   f" ({max_viol._usina}) = 0")
                    reg._modificacao._dados = 0
                except ValueError:
                    self._log.warning("Flexibilizando FP - " +
                                      "Não foi encontrado registro AC VERTJU" +
                                      f" para a usina {max_viol._codigo} " +
                                      f" ({max_viol._usina})")
                    reg_ac_novo = AC()
                    reg_ac_novo.uhe = max_viol._codigo
                    reg_ac_novo.modificacao = "VERTJU"
                    reg_ac_novo._modificacao = ACVERTJU("")
                    reg_ac_novo._modificacao._dados = 0
                    dadger.cria_registro(dadger.ac(169, "NPOSNW"),
                                         reg_ac_novo)

            except ValueError:
                self._log.warning("Flexibilizando FP - " +
                                  "Não foi encontrado registro FP" +
                                  f" para a usina {max_viol._codigo} " +
                                  f" ({max_viol._usina})")
                reg_fp_novo = FP()
                reg_fp_novo.codigo = max_viol._codigo
                reg_fp_novo.estagio = 1
                reg_fp_novo.tipo_entrada_janela_turbinamento = 0
                reg_fp_novo.numero_pontos_turbinamento = 20
                reg_fp_novo.limite_inferior_janela_turbinamento = 0
                reg_fp_novo.limite_superior_janela_turbinamento = 100
                reg_fp_novo.tipo_entrada_janela_volume = 0
                reg_fp_novo.numero_pontos_volume = 20
                reg_fp_novo.limite_inferior_janela_volume = 100
                reg_fp_novo.limite_superior_janela_volume = 100
                dadger.cria_registro(dadger.fc("NEWCUT"),
                                     reg_fp_novo)

    # Override
    def _flexibilizaDEFMIN(self,
                           dadger: Dadger,
                           inviabilidades: List[InviabilidadeDEFMIN]):

        def __identifica_inv(inv: InviabilidadeDEFMIN) -> Tuple[int, int, int]:
            return (inv._codigo, inv._estagio, inv._patamar)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeDEFMIN],
                                               inv_ini: InviabilidadeDEFMIN
                                               ) -> InviabilidadeDEFMIN:
            max_viol = inv_ini
            ident_ini = __identifica_inv(inv_ini)
            invs_mesma_id = [i for i in invs if
                             __identifica_inv(i) == ident_ini]
            for i in invs_mesma_id:
                if i._violacao > max_viol._violacao:
                    max_viol = i
            return max_viol

        # Estrutura para conter os pares (código, estágio, patamar)
        # já flexibilizados
        flexibilizados: List[Tuple[int, int, int]] = []
        for inv in inviabilidades:
            identificacao = __identifica_inv(inv)
            # Se já flexibilizou essa restrição nesse estágio, ignora
            if identificacao in flexibilizados:
                continue
            # Senão, procura dentre todas as outras pela maior violação
            flexibilizados.append(identificacao)
            max_viol = __inv_maxima_violacao_identificada(inviabilidades,
                                                          inv)
            # Procura por um registro AC de VAZMIN
            try:
                reg = dadger.ac(max_viol._codigo, "VAZMIN")
            except ValueError:
                self._log.warning("Flexibilizando DEFMIN - " +
                                  "Não foi encontrado registro AC VAZMIN" +
                                  f" para a usina {max_viol._codigo} " +
                                  f" ({max_viol._usina})")
                reg_ac_novo = AC()
                reg_ac_novo.uhe = max_viol._codigo
                reg_ac_novo.modificacao = "VAZMIN"
                reg_ac_novo._modificacao = ACVAZMIN("")
                reg_ac_novo._modificacao._dados = max_viol._vazmin_hidr
                dadger.cria_registro(dadger.ac(169, "NPOSNW"),
                                     reg_ac_novo)
                reg = reg_ac_novo

            # Flexibiliza
            valor_flex = int(np.ceil(max_viol._violacao))
            novo_valor = np.max([0, reg._modificacao._dados - valor_flex])
            self._log.info(f"Flexibilizando DEFMIN {max_viol._codigo} -" +
                           f" Estágio {max_viol._estagio}:" +
                           f" {reg._modificacao._dados} -> {novo_valor}")
            reg._modificacao._dados = novo_valor

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

        def __identifica_inv_pat(inv: InviabilidadeDeficit) -> Tuple[int,
                                                                     int,
                                                                     str]:
            return (inv._estagio, inv._patamar, inv._subsistema)

        def __identifica_inv(inv: InviabilidadeDeficit) -> Tuple[int, str]:
            return (inv._estagio, inv._subsistema)

        def __inv_maxima_violacao_identificada(invs: List[InviabilidadeDeficit],
                                               inv_ini: InviabilidadeDeficit
                                               ) -> InviabilidadeDeficit:
            max_viol = inv_ini
            ident_ini = __identifica_inv(inv_ini)
            invs_mesma_id = [i for i in invs if
                             __identifica_inv(i) == ident_ini]
            for i in invs_mesma_id:
                max_viol._violacao_percentual += i._violacao_percentual
            return max_viol

        # TODO - não precisar dessa constante de mapeamento SUB-REE
        rees_subsistema = {
            "SE": [1, 5, 6, 7, 10, 12],
            "S": [2, 11],
            "NE": [3],
            "N": [4, 8, 9]
        }

        # Estrutura para conter as tuplas
        # (código, estágio, limite) já flexibilizados
        flexibilizados: List[Tuple[int, int, str]] = []
        for inv in inviabilidades:
            # Ignora os cenários do 2º mês
            if inv._estagio == dadger.lista_registros(DP)[-1].estagio:
                continue
            identificacao = __identifica_inv(inv)
            # Se já flexibilizou essa restrição nesse estágio, ignora
            if identificacao in flexibilizados:
                continue
            # Senão, procura dentre todas as outras pela maior violação
            flexibilizados.append(identificacao)
            max_viol = __inv_maxima_violacao_identificada(inviabilidades,
                                                          inv)
            # Tenta flexibilizar todos os REEs daquele subsistema, que tiverem
            # restrições RHE
            for r in rees_subsistema[max_viol._subsistema]:
                # Lista as restrições RHE
                cms = dadger.lista_registros(CM)
                # O atributo estagio na verdade é o REE
                cms_ree = [c for c in cms if c.estagio == r]
                # Se tiver pelo menos um CM para o REE, flexibiliza os
                # RHE que existirem, para os respectivos estágios
                if len(cms_ree) > 0:
                    for cm in cms_ree:
                        try:
                            reg = dadger.he(cm.codigo, max_viol._estagio)
                            valor_atual = reg.limite
                            deltas = RegraFlexibilizacao.deltas_inviabilidades
                            delta = deltas[InviabilidadeDeficit]
                            valor_flex = max_viol._violacao_percentual + delta
                            novo_valor = max([0., valor_atual - valor_flex])
                            dadger.he(cm.codigo, max_viol._estagio).limite = novo_valor
                            msg = (f"Flexibilizando (DEFICIT) HE {reg.codigo} -" +
                                   f"Estágio {max_viol._estagio} -" +
                                   f"{reg.tipo_penalidade}: {valor_atual} ->" +
                                   f" {novo_valor}")
                            self._log.info(msg)
                            if novo_valor == 0:
                                self._log.warning(f"Valor da HE {reg.codigo} chegou a" +
                                                  " 0. e deveria ser" +
                                                  f" {valor_atual - valor_flex}" +
                                                  " pelo déficit")
                        except ValueError:
                            continue
