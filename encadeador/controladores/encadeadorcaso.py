from abc import abstractmethod
from logging import Logger
from typing import List
import pandas as pd
from idecomp.decomp.relato import Relato
from idecomp.decomp.dadger import Dadger
from inewave.newave import Confhd  # type: ignore

from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.utils.terminal import converte_codificacao


class Encadeador:

    def __init__(self,
                 caso_anterior: Caso,
                 caso_atual: Caso,
                 log: Logger):
        self._caso_anterior = caso_anterior
        self._caso_atual = caso_atual
        self._log = log

    @staticmethod
    def factory(caso_anterior: Caso,
                caso_atual: Caso,
                log: Logger) -> 'Encadeador':
        if isinstance(caso_atual, CasoDECOMP):
            return EncadeadorDECOMPDECOMP(caso_anterior,
                                          caso_atual,
                                          log)
        elif isinstance(caso_atual, CasoNEWAVE):
            return EncadeadorDECOMPNEWAVE(caso_anterior,
                                          caso_atual,
                                          log)
        else:
            raise TypeError(f"Caso do tipo {type(caso_atual)} " +
                            "não suportado para encadeamento")

    @abstractmethod
    def encadeia(self) -> bool:
        pass


class EncadeadorDECOMPNEWAVE(Encadeador):

    def __init__(self,
                 caso_anterior: Caso,
                 caso_atual: Caso,
                 log: Logger):
        super().__init__(caso_anterior, caso_atual, log)

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
                return list(volumes.columns)[-1]

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

    def encadeia(self) -> bool:
        self._log.info(f"Encadeando casos: {self._caso_anterior.nome} -> " +
                       f"{self._caso_atual.nome}")
        v = self._caso_atual.configuracoes.variaveis_encadeadas
        if "EARM" in v:
            self.__encadeia_earm()
        return True


class EncadeadorDECOMPDECOMP(Encadeador):

    def __init__(self,
                 caso_anterior: Caso,
                 caso_atual: Caso,
                 log: Logger):
        super().__init__(caso_anterior, caso_atual, log)

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

    def encadeia(self) -> bool:
        self._log.info(f"Encadeando casos: {self._caso_anterior.nome} -> " +
                       f"{self._caso_atual.nome}")
        v = self._caso_atual.configuracoes.variaveis_encadeadas
        if "EARM" in v:
            self.__encadeia_earm()
        if "TVIAGEM" in v:
            self.__encadeia_tviagem()
        return True
