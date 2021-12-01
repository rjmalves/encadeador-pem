from abc import abstractmethod
from logging import Logger
from typing import List
import pandas as pd
import numpy as np
from idecomp.decomp.relato import Relato
from idecomp.decomp.relgnl import RelGNL
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.dadgnl import DadGNL
from idecomp.decomp.modelos.dadgnl import NL, GL
from inewave.newave import Confhd
from inewave.newave import PMO
from inewave.newave import EafPast
from inewave.newave.adterm import AdTerm
from inewave.config import MESES_DF

from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.utils.terminal import converte_codificacao


class Encadeador:

    def __init__(self,
                 casos_anteriores: List[Caso],
                 caso_atual: Caso,
                 log: Logger):

        def __decomp_anterior():
            return [c for c in reversed(casos_anteriores)
                    if isinstance(c, CasoDECOMP)][0]

        self._casos_anteriores = casos_anteriores
        self._caso_anterior = __decomp_anterior()
        self._caso_atual = caso_atual
        self._log = log

    @staticmethod
    def factory(casos_anteriores: List[Caso],
                caso_atual: Caso,
                log: Logger) -> 'Encadeador':
        if isinstance(caso_atual, CasoDECOMP):
            return EncadeadorDECOMPDECOMP(casos_anteriores,
                                          caso_atual,
                                          log)
        elif isinstance(caso_atual, CasoNEWAVE):
            return EncadeadorDECOMPNEWAVE(casos_anteriores,
                                          caso_atual,
                                          log)
        else:
            raise TypeError(f"Caso do tipo {type(caso_atual)} " +
                            "não suportado para encadeamento")

    @property
    def newaves_anteriores(self) -> List[CasoNEWAVE]:
        return [c for c in self._casos_anteriores
                if isinstance(c, CasoNEWAVE)]

    @property
    def decomps_anteriores(self) -> List[CasoDECOMP]:
        return [c for c in self._casos_anteriores
                if isinstance(c, CasoDECOMP)]

    @abstractmethod
    def encadeia(self) -> bool:
        pass


class EncadeadorDECOMPNEWAVE(Encadeador):

    def __init__(self,
                 casos_anteriores: List[Caso],
                 caso_atual: Caso,
                 log: Logger):
        super().__init__(casos_anteriores, caso_atual, log)

    def __encadeia_earm(self):

        def __numero_uhe_decomp(numero_newave: int) -> int:
            mapa_ficticias_NW_DC = {
                                    318: 122,
                                    319: 57,
                                    294: 162,
                                    295: 156,
                                    308: 155,
                                    298: 148,
                                    292: 252,
                                    302: 261,
                                    303: 257,
                                    306: 253
                                   }
            if numero_newave in mapa_ficticias_NW_DC.keys():
                return mapa_ficticias_NW_DC[numero_newave]
            else:
                return numero_newave

        def __correcao_serra_mesa_ficticia(vol: float) -> float:
            return min([100.0, vol / 0.55])

        def __separou_ilha_solteira_equiv(volumes: pd.DataFrame,
                                          usinas: pd.DataFrame) -> bool:
            # Saber se tem I. Solteira Equiv. no DECOMP mas tem as
            # usinas separadas no NEWAVE
            usinas_newave = usinas["Número"].tolist()
            usinas_decomp = volumes["Número"].tolist()
            return all([
                        44 not in usinas_newave,
                        43 in usinas_newave,
                        34 in usinas_newave,
                        44 in usinas_decomp,
                        43 not in usinas_decomp,
                        34 not in usinas_decomp
                       ])

        def __encadeia_ilha_solteira_equiv(volumes: pd.DataFrame,
                                           usinas: pd.DataFrame
                                           ) -> pd.DataFrame:
            vol = float(volumes.loc[volumes["Número"] == 44,
                                    __coluna_para_encadear()])
            self._log.info(f"Caso especial de I. Solteira Equiv: {vol} %")
            usinas.loc[usinas["Número"] == 34,
                       "Volume Inicial"] = vol
            usinas.loc[usinas["Número"] == 43,
                       "Volume Inicial"] = vol
            return usinas

        def __coluna_para_encadear() -> str:
            if self._caso_atual.revisao == 0:
                return "Estágio 1"
            else:
                return list(volumes.columns)[-2]

        def __interpola_volume() -> float:
            # TODO - implementar para maior precisão
            pass

        self._log.info("Encadeando EARM")
        # Lê o relato do DC
        # TODO - tratar casos de arquivos não encontrados
        arq_relato = f"relato.rv{self._caso_anterior.revisao}"
        relato = Relato.le_arquivo(self._caso_anterior.caminho,
                                   arq_relato)
        volumes = relato.volume_util_reservatorios
        # Lê o confhd do NW
        confhd = Confhd.le_arquivo(self._caso_atual.caminho)
        usinas = confhd.usinas

        # Atualiza cada armazenamento
        for _, linha in usinas.iterrows():
            num = linha["Número"]
            num_dc = __numero_uhe_decomp(num)
            # Confere se tem o reservatório
            if num_dc not in set(volumes["Número"]):
                continue
            vol = float(volumes.loc[volumes["Número"] == num_dc,
                                    __coluna_para_encadear()])
            if num_dc == 251:
                vol_fict = __correcao_serra_mesa_ficticia(vol)
                self._log.info("Correção de Serra da Mesa fictícia: " +
                               f"{vol} -> {vol_fict}")
                usinas.loc[usinas["Número"] == 291,
                           "Volume Inicial"] = vol_fict
            usinas.loc[usinas["Número"] == num,
                       "Volume Inicial"] = vol

        # Trata o caso de I. Solteira Equiv.
        if __separou_ilha_solteira_equiv(volumes, usinas):
            usinas = __encadeia_ilha_solteira_equiv(volumes, usinas)

        # Escreve o confhd de saída
        confhd.escreve_arquivo(self._caso_atual.caminho)

    def __encadeia_ena(self):

        def __interpola_mes_vigente():
            # Remove a coluna de ENA prevista do 2º mês
            ena_previsao = relato.ena_acoplamento_ree
            ena_previsao = ena_previsao.loc[ena_previsao["Cenário"] == 1, :]
            cols_ena_previsao = list(ena_previsao.columns)
            cols_ena_previsao = cols_ena_previsao[:-1]
            ena_previsao = ena_previsao[cols_ena_previsao]
            cols_ena_previsao = [c for c in cols_ena_previsao
                                 if "Estágio" in c]
            # enas_previstas é um DF apenas com as colunas de ENA prevista
            # para cada semana. É ordenado de maneira cronológica.
            enas_previstas = ena_previsao[cols_ena_previsao]
            enas_previstas.reset_index(drop=True, inplace=True)
            # Prepara as ENAs verificadas
            ena_pre_estudo = relato.ena_pre_estudo_semanal_ree
            cols_ena_verif = list(ena_pre_estudo.columns)
            cols_ena_verif = [c for c in cols_ena_verif
                              if "Estágio Pré" in c]
            enas_verificadas = ena_pre_estudo[reversed(cols_ena_verif)]
            # enas_verificadas é um DF apenas com as colunas de ENA verificada
            # para cada semana. É ordenado de maneira cronológica.
            enas_semanas = pd.concat([enas_previstas,
                                      enas_verificadas],
                                     axis=1)
            n_semanas = len(enas_semanas.columns)
            enas_semanas.columns = [f"Semana {i}" for i in
                                    range(1, n_semanas + 1)]
            # enas_semanas é um DF apenas com as semanas, organizadas de maneira
            # cronológia, pronto para ponderar.
            dias_semana_inic = relato.dias_excluidos_semana_inicial
            dias_semana_fin = relato.dias_excluidos_semana_final
            # Corrige os pesos de cada semana com os dias excluídos
            pesos_semanas = 7 * np.ones((n_semanas,))
            pesos_semanas[0] -= dias_semana_inic
            pesos_semanas[-1] -= dias_semana_fin
            pesos_totais = np.sum(pesos_semanas)
            ena_ponderada = pesos_semanas * enas_semanas / pesos_totais
            return ena_ponderada.sum(axis=1)

        # Não faz nada nas RV0
        if self._caso_atual.revisao == 0:
            return
        # Senão, encadeia ENA
        self._log.info("Encadeando ENA")
        mes_caso = self._caso_atual.mes
        ultimo_newave = self.newaves_anteriores[-1]
        ultimo_decomp = self.decomps_anteriores[-1]
        # TODO - tratar casos de arquivos não encontrados
        # Lê o pmo.dat do último NEWAVE
        pmo = PMO.le_arquivo(ultimo_newave.caminho)
        eafpast_pmo = pmo.eafpast_tendencia_hidrologica
        # Lê o eafpast do próximo NEWAVE
        eafpast = EafPast.le_arquivo(self._caso_atual.caminho)
        # Substitui pelos valores do pmo.dat
        eafpast.tendencia[MESES_DF] = eafpast_pmo[MESES_DF]
        # Lê o relato.rvX do DECOMP
        arq_relato = f"relato.rv{ultimo_decomp.revisao}"
        relato = Relato.le_arquivo(ultimo_decomp.caminho,
                                   arq_relato)
        # Interpola o mês vigente e substitui no eafpast
        mes_eafpast = MESES_DF[mes_caso - 1]
        eafpast.tendencia[mes_eafpast] = __interpola_mes_vigente()
        # Escreve o Eafpast
        eafpast.escreve_arquivo(self._caso_atual.caminho)

    def __encadeia_gnl(self):
        ultimo_rv0 = None
        for c in reversed(self._casos_anteriores):
            if isinstance(c, CasoNEWAVE) and c.revisao == 0:
                ultimo_rv0 = c
                break
        if ultimo_rv0 is None:
            self._log.error("Último NW rv0 não encontrado para " +
                            "encadeamento de GNL")
            raise RuntimeError()
        # Lê o AdTerm do caso atual
        adterm = AdTerm.le_arquivo(self._caso_atual.caminho)
        # Lê o AdTerm do último NEWAVE rv0
        adterm_rv0 = AdTerm.le_arquivo(ultimo_rv0.caminho)
        d = adterm.despachos
        self._log.info(d)
        d_rv0 = adterm_rv0.despachos
        indices_usinas = d["Índice UTE"].unique()
        cols_patamares = [f"Patamar {i}" for i in [1, 2, 3]]
        for u in indices_usinas:
            self._log.info(f"NWNW - Atualizando GNL usina {u}")
            if u not in d_rv0["Índice UTE"].tolist():
                continue
            filtro_d = (d["Índice UTE"] == u) & (d["Lag"] == 1)
            filtro_d_rv0 = (d_rv0["Índice UTE"] == u) & (d_rv0["Lag"] == 2)
            d.loc[filtro_d,
                  cols_patamares] = d_rv0.loc[filtro_d_rv0,
                                              cols_patamares].to_numpy()
        self._log.info(d)
        # Lê o RelGNL do último decomp
        ultimo_dc = None
        for c in reversed(self._casos_anteriores):
            if isinstance(c, CasoDECOMP):
                ultimo_dc = c
                break
        if ultimo_dc is None:
            self._log.error("Último DC não encontrado para " +
                            "encadeamento de GNL")
            raise RuntimeError()
        nome_rel = f"relgnl.rv{ultimo_dc.revisao}"
        rel = RelGNL.le_arquivo(ultimo_dc.caminho, nome_rel)
        codigos = rel.usinas_termicas["Código"].unique()
        usinas = rel.usinas_termicas["Usina"].unique()
        mapa_codigo_usina = {c: u for c, u in zip(codigos, usinas)}
        cols_despacho = [f"Despacho Pat. {i}" for i in [1, 2, 3]]
        op = rel.relatorio_operacao_termica
        for u in indices_usinas:
            self._log.info(f"NWDC - Atualizando GNL usina {u}")
            if u not in mapa_codigo_usina:
                continue
            nome = mapa_codigo_usina[u]
            d_dc = op.loc[(op["Usina"] == nome) &
                          (op["Estágio"] == "MENSAL"),
                          cols_despacho].to_numpy()
            filtro_d = (d["Índice UTE"] == u) & (d["Lag"] == 2)
            d.loc[filtro_d,
                  cols_patamares] = d_dc
        self._log.info(d)
        # Escreve o arquivo de saída
        adterm.escreve_arquivo(self._caso_atual.caminho)

    def encadeia(self) -> bool:
        self._log.info(f"Encadeando casos: {self._caso_anterior.nome} -> " +
                       f"{self._caso_atual.nome}")
        v = self._caso_atual.configuracoes.variaveis_encadeadas
        if "EARM" in v:
            self.__encadeia_earm()
        if "ENA" in v:
            self.__encadeia_ena()
        if "GNL" in v:
            self.__encadeia_gnl()
        return True


class EncadeadorDECOMPDECOMP(Encadeador):

    def __init__(self,
                 casos_anteriores: List[Caso],
                 caso_atual: Caso,
                 log: Logger):
        super().__init__(casos_anteriores, caso_atual, log)

    def __encadeia_earm(self):
        self._log.info("Encadeando EARM")

        def __separou_ilha_solteira_equiv(volumes: pd.DataFrame,
                                          dadger: Dadger) -> bool:
            # Saber se tem I. Solteira Equiv. no DECOMP mas tem as
            # usinas separadas no próximo DECOMP
            vols_relato = volumes["Número"].tolist()

            existe_equiv_relato = 44 in vols_relato
            existem_separadas_relato = all([34 in vols_relato,
                                            43 in vols_relato])
            try:
                dadger.uh(44)
                existe_equiv_dadger = True
            except ValueError:
                existe_equiv_dadger = False
            try:
                dadger.uh(34)
                dadger.uh(43)
                existem_separadas_dadger = True
            except ValueError:
                existem_separadas_dadger = False

            return all([
                        existe_equiv_relato,
                        not existem_separadas_relato,
                        not existe_equiv_dadger,
                        existem_separadas_dadger
                       ])

        def __encadeia_ilha_solteira_equiv(volumes: pd.DataFrame,
                                           dadger: Dadger):
            vol = float(volumes.loc[volumes["Número"] == 44,
                                    "Estágio 1"])
            self._log.info(f"Caso especial de I. Solteira Equiv: {vol} %")
            dadger.uh(34).volume_inicial = vol
            dadger.uh(43).volume_inicial = vol

        # Lê o relato do DC anterior
        # TODO - tratar casos de arquivos não encontrados
        arq_relato = f"relato.rv{self._caso_anterior.revisao}"
        relato = Relato.le_arquivo(self._caso_anterior.caminho,
                                   arq_relato)
        volumes = relato.volume_util_reservatorios
        # Lê o dadger do DC atual
        conv = self._caso_atual.configuracoes.script_converte_codificacao
        converte_codificacao(self._caso_atual.caminho,
                             conv)
        arq_dadger = f"dadger.rv{self._caso_atual.revisao}"
        dadger = Dadger.le_arquivo(self._caso_atual.caminho,
                                   arq_dadger)
        # Encadeia cada armazenamento
        for _, linha in volumes.iterrows():
            num = linha["Número"]

            # Caso especial de I. Solteira Equiv.
            if num == 44 and __separou_ilha_solteira_equiv(volumes, dadger):
                __encadeia_ilha_solteira_equiv(volumes, dadger)
                continue

            vol = float(volumes.loc[volumes["Número"] == num,
                                    "Estágio 1"])
            dadger.uh(num).volume_inicial = vol

        # Escreve o dadger de saída
        dadger.escreve_arquivo(self._caso_atual.caminho,
                               arq_dadger)

    def __encadeia_tviagem(self):

        def __codigos_usinas_tviagem() -> List[int]:
            return [156, 162]

        self._log.info("Encadeando TVIAGEM")
        # Lê o relato do DC anterior
        # TODO - tratar casos de arquivos não encontrados
        arq_relato = f"relato.rv{self._caso_anterior.revisao}"
        relato = Relato.le_arquivo(self._caso_anterior.caminho,
                                   arq_relato)
        relatorio = relato.relatorio_operacao_uhe
        # Lê o dadger do DC atual
        conv = self._caso_atual.configuracoes.script_converte_codificacao
        converte_codificacao(self._caso_atual.caminho,
                             conv)
        arq_dadger = f"dadger.rv{self._caso_atual.revisao}"
        dadger = Dadger.le_arquivo(self._caso_atual.caminho,
                                   arq_dadger)
        # Encadeia cada tempo de viagem
        for codigo in __codigos_usinas_tviagem():
            # Extrai o Qdef do relato
            qdef = float(relatorio.loc[(relatorio["Estágio"] == 1) &
                                       (relatorio["Código"] == codigo),
                                       "Qdef (m3/s)"])
            # Atualiza os tempos de viagem no dadger
            vi = dadger.vi(codigo)
            vi.vazoes = [qdef] + vi.vazoes[:-1]

        # Escreve o dadger de saída
        dadger.escreve_arquivo(self._caso_atual.caminho,
                               arq_dadger)

    def __encadeia_gnl(self):
        # Lê o DadGNL do decomp atual
        nome_dad = f"dadgnl.rv{self._caso_atual.revisao}"
        dad = DadGNL.le_arquivo(self._caso_atual.caminho, nome_dad)
        # Lê o RelGNL e DadGNL do último decomp
        ultimo_dc = None
        for c in reversed(self._casos_anteriores):
            if isinstance(c, CasoDECOMP):
                ultimo_dc = c
                break
        if ultimo_dc is None:
            self._log.error("Último DC não encontrado para " +
                            "encadeamento de GNL")
            raise RuntimeError()
        nome_rel = f"relgnl.rv{ultimo_dc.revisao}"
        nome_dad_anterior = f"dadgnl.rv{ultimo_dc.revisao}"
        rel = RelGNL.le_arquivo(ultimo_dc.caminho, nome_rel)
        dad_anterior = DadGNL.le_arquivo(ultimo_dc.caminho, nome_dad_anterior)

        cods = rel.usinas_termicas["Código"].unique()
        usinas = rel.usinas_termicas["Usina"].unique()
        mapa_codigo_usina = {c: u for c, u in zip(cods, usinas)}

        codigos = [r.codigo for r in dad.lista_registros(NL)]
        registros = dad.lista_registros(GL)
        registros_anteriores = dad_anterior.lista_registros(GL)
        for c in codigos:
            # Para cada semana i (exceto a última), o registro GL do DadGNL do
            # caso atual deve ter o valor do respectivo registro GL do DadGNL
            # do caso anterior na semana i + 1
            registros_usina = [r for r in registros if r.codigo == c]
            registros_usina_anterior = [r for r in registros_anteriores
                                        if r.codigo == c]
            cols_despacho = [f"Despacho Pat. {i}" for i in [1, 2, 3]]
            for r in registros_usina:
                # Para a última semana, o registro GL do DadGNL atual deve vir
                # do RelGNL do caso anterior, onde a semana de início tenha o
                # mesmo valor.
                if r == registros_usina[-1]:
                    op = rel.relatorio_operacao_termica
                    data = r.dados[-1]
                    data = data[:2] + "/" + data[2:4] + "/" + data[4:]
                    # Procura pela linha em op filtrando por nome, data
                    # e pegando as colunas dos despachos
                    nome = mapa_codigo_usina[c]
                    filtro = (op["Usina"] == nome) & (op["Início Semana"] == data)
                    self._log.info(f"Debug GNL: {op.loc[filtro, cols_despacho]}")
                    geracoes = op.loc[filtro, cols_despacho].to_numpy()
                    self._log.info(f"Debug GNL: {geracoes}")
                    self._log.info(f"Encadeando GNL: {[g for g in geracoes]}")
                    r.geracoes = [g for g in geracoes]
                else:
                    # Procura pelo registro anterior com a mesma data
                    reg_ant = [ra for ra in registros_usina_anterior
                               if ra.dados[-1] == r.dados[-1]][0]
                    self._log.info(f"Encadeando GNL: {reg_ant.geracoes}")
                    r.geracoes = reg_ant.geracoes

        # Escreve o arquivo de saída
        dad.escreve_arquivo(self._caso_atual.caminho, nome_dad)

    def encadeia(self) -> bool:
        self._log.info(f"Encadeando casos: {self._caso_anterior.nome} -> " +
                       f"{self._caso_atual.nome}")
        v = self._caso_atual.configuracoes.variaveis_encadeadas
        if "EARM" in v:
            self.__encadeia_earm()
        if "TVIAGEM" in v:
            self.__encadeia_tviagem()
        if "GNL" in v:
            self.__encadeia_gnl()
        return True
