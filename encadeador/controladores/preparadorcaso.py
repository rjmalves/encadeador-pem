from abc import abstractmethod
from os.path import join
from logging import Logger
from typing import Optional

from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.controladores.sintetizadorcaso import SintetizadorCasoNEWAVE
from inewave.newave import DGer  # type: ignore
from idecomp.decomp.dadger import Dadger  # type: ignore


class PreparadorCaso:

    def __init__(self,
                 caso: Caso) -> None:
        self._caso = caso

    @abstractmethod
    def prepara_caso(self,
                     log: Logger,
                     **kwargs) -> bool:
        pass

    @abstractmethod
    def encadeia_variaveis(self,
                           caso_anterior: Optional[Caso],
                           log: Logger) -> bool:
        pass

    @property
    def caso(self) -> Caso:
        return self._caso


class PreparadorCasoNEWAVE(PreparadorCaso):

    def __init__(self, caso: CasoNEWAVE) -> None:
        super().__init__(caso)

    def prepara_caso(self,
                     log: Logger,
                     **kwargs) -> bool:
        log.info(f"Adequando caso do NEWAVE: {self.caso.nome}")
        try:

            # TODO
            if True:
                # Adequa o nome do caso
                nome_estudo = self.caso.configuracoes.nome_estudo
                ano = self.caso.ano
                mes = self.caso.mes
                dger = DGer.le_arquivo(self.caso.caminho)
                log.info("DGer lido com sucesso")
                dger.nome_caso = f"{nome_estudo} - NW {mes}/{ano}"
                # Adequa parâmetros de CVAR
                # TODO
                # Adequa opção do PAR(p)-A
                parpa = dger.afluencia_anual_parp
                # TODO
                parpa[0] = 3
                dger.afluencia_anual_parp = parpa
                log.info(f"Opção do PAR(p)-A alterada para {parpa}")
                # Salva o deck de entrada
                dger.escreve_arquivo(self.caso.caminho)
                log.info("Adequação do caso concluída com sucesso")
            return True
        except FileNotFoundError as e:
            log.error(f"Erro na leitura do deck de entrada: {e}")
            return False

    def encadeia_variaveis(self,
                           caso_anterior: Optional[Caso],
                           log: Logger) -> bool:
        if caso_anterior is None:
            log.info(f"Primeiro NW: {self.caso.nome} - sem encadeamentos")
            return True
        elif isinstance(caso_anterior, CasoDECOMP):
            log.info("Encadeando variáveis dos casos ",
                     f"{caso_anterior.nome} -> {self.caso.nome}")
            # TODO - Encadeia as variáveis selecionadas
            return True
        else:
            log.error("Encadeamento NW com NW não suportado. Casos: " +
                      f"{caso_anterior.nome} -> {self.caso.nome}")
            return False


class PreparadorCasoDECOMP(PreparadorCaso):

    def __init__(self, caso: CasoDECOMP) -> None:
        super().__init__(caso)

    def prepara_caso(self,
                     log: Logger,
                     **kwargs) -> bool:
        log.info(f"Adequando caso do DECOMP: {self.caso.nome}")
        try:
            dadger = Dadger.le_arquivo(self.caso.caminho,
                                       f"dadger.rv{self.caso.revisao}")
            log.info("Dadger lido com sucesso")
            if True:
                # Adequa registro TE
                nome_estudo = self.caso.configuracoes.nome_estudo
                ano = self.caso.ano
                mes = self.caso.mes
                rv = self.caso.revisao
                dadger.te.titulo = f"{nome_estudo} - DC {mes}/{ano} RV{rv}"
                # Adequa registro NI
                n_iter = self.caso.configuracoes.maximo_iteracoes_decomp
                dadger.ni.iteracoes = n_iter
                # Adequa registro GP
                # TODO
                # Adequa os registros FC (cortes e cortesh)
                caso_entrada = kwargs.get("caso_cortes")
                if caso_entrada is None or not isinstance(caso_entrada,
                                                          CasoNEWAVE):
                    log.error("Erro na especificação dos cortes da FCF")
                    return False
                caso_cortes: CasoNEWAVE = caso_entrada
                # Verifica se é necessário e extrai os cortes
                sintetizador = SintetizadorCasoNEWAVE(caso_cortes, log)
                if not sintetizador.verifica_cortes_extraidos():
                    sintetizador.extrai_cortes()
                # Altera os registros FC
                deck = DeckEntrada.le_deck(caso_cortes.caminho)
                dadger.fc("NEWV21").caminho = join(caso_cortes.caminho,
                                                   deck.arquivos.cortesh)
                dadger.fc("NEWCUT").caminho = join(caso_cortes.caminho,
                                                   deck.arquivos.cortes)
                # Salva o dadger
                dadger.escreve_arquivo(self.caso.caminho,
                                       f"dadger.rv{self.caso.revisao}")
                log.info("Adequação do caso concluída com sucesso")
            return True
        except FileNotFoundError as e:
            log.error(f"Erro na leitura do dadger: {e}")
            return False

    def encadeia_variaveis(self,
                           caso_anterior: Optional[Caso],
                           log: Logger) -> bool:
        if caso_anterior is None:
            log.info(f"Primeiro DC: {self.caso.nome} - sem encadeamentos")
            return True
        elif isinstance(caso_anterior, CasoDECOMP):
            log.info("Encadeando variáveis dos casos ",
                     f"{caso_anterior.nome} -> {self.caso.nome}")
            # TODO - Encadeia as variáveis selecionadas
            return True
        else:
            log.error("Encadeamento NW com DC não suportado. Casos: " +
                      f"{caso_anterior.nome} -> {self.caso.nome}")
            return False
