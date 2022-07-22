from abc import abstractmethod
from os.path import join
from typing import List

from encadeador.modelos.caso2 import Caso
from encadeador.controladores.encadeadorcaso import Encadeador
from encadeador.controladores.sintetizadorcaso import SintetizadorNEWAVE
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.programa import Programa
from encadeador.modelos.regrareservatorio import RegraReservatorio
from encadeador.controladores.aplicadorregrasreservatorios import (
    AplicadorRegrasReservatorios,
)
from encadeador.services.unitofwork.newave import FSNewaveUnitOfWork
from encadeador.utils.terminal import converte_codificacao
from encadeador.utils.log import Log
from inewave.newave import DGer, Arquivos, CVAR  # type: ignore
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.modelos.dadger import RT


class PreparadorCaso:
    def __init__(self, caso: Caso, casos_anteriores: List[Caso]) -> None:
        self._caso = caso
        self._casos_anteriores = casos_anteriores

    @staticmethod
    def factory(caso: Caso, casos_anteriores: List[Caso]) -> "PreparadorCaso":
        if caso.programa == Programa.NEWAVE:
            return PreparadorNEWAVE(caso, casos_anteriores)
        elif caso.programa == Programa.DECOMP:
            return PreparadorDECOMP(caso, casos_anteriores)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    @abstractmethod
    def prepara(self, **kwargs) -> bool:
        pass

    @abstractmethod
    def encadeia(self) -> bool:
        pass

    @abstractmethod
    def aplica_regras_operacao_reservatorios(
        self,
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
        pass

    @property
    def caso(self) -> Caso:
        return self._caso


class PreparadorNEWAVE(PreparadorCaso):
    def __init__(self, caso: Caso, casos_anteriores: List[Caso]) -> None:
        super().__init__(caso, casos_anteriores)

    def __deleta_cortes_ultimo_newave(self):
        for c in reversed(self._casos_anteriores):
            if c.programa == Programa.NEWAVE:
                uow = FSNewaveUnitOfWork(c.caminho)
                with uow:
                    Log.log().info(
                        "Deletando cortes do último NEWAVE: " + f"{c.caminho}"
                    )
                    uow.deleta_cortes()

    def __adequa_dger(self, dger: DGer):
        nome_estudo = Configuracoes().nome_estudo
        ano = self.caso.ano
        mes = self.caso.mes
        dger.nome_caso = f"{nome_estudo} - NW {mes}/{ano}"
        opcao_parpa = Configuracoes().opcao_parpa
        dger.consideracao_media_anual_afluencias = opcao_parpa
        Log.log().info(f"Opção do PAR(p)-A alterada: {opcao_parpa}")

    def __adequa_cvar(self, cvar: CVAR):
        par_cvar = Configuracoes().cvar
        cvar.valores_constantes = par_cvar
        Log.log().info(f"Valores de CVAR alterados: {par_cvar}")

    def prepara(self) -> bool:
        Log.log().info(f"Preparando caso do NEWAVE: {self.caso.nome}")
        self.__deleta_cortes_ultimo_newave()
        uow = FSNewaveUnitOfWork(self.caso.caminho)
        with uow:
            if Configuracoes().adequa_decks_newave:
                dger = uow.newave.get_dger()
                self.__adequa_dger(dger)
                uow.newave.set_dger(dger)
                cvar = uow.newave.get_cvar()
                self.__adequa_cvar(cvar)
                uow.newave.set_cvar(cvar)
            Log.log().info("Adequação do caso concluída com sucesso")
            return True

    def encadeia(self) -> bool:
        if len(self._casos_anteriores) == 0:
            Log.log().info(f"Primeiro: {self.caso.nome} - sem encadeamentos")
            return True
        elif self._casos_anteriores[-1].programa == Programa.DECOMP:
            Log.log().info(
                "Encadeando variáveis dos casos "
                + f"{self._casos_anteriores[-1].nome} -> {self.caso.nome}"
            )
            encadeador = Encadeador.factory(self._casos_anteriores, self.caso)
            return encadeador.encadeia()
        else:
            Log.log().error(
                "Encadeamento NW com NW não suportado. Casos: "
                + f"{self._casos_anteriores[-1].nome} -> {self.caso.nome}"
            )
            return False

    def aplica_regras_operacao_reservatorios(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
        aplicador = AplicadorRegrasReservatorios.factory(self.caso)
        return aplicador.aplica_regras(casos_anteriores, regras_operacao)


class PreparadorDECOMP(PreparadorCaso):
    def __init__(self, caso: Caso, casos_anteriores: List[Caso]) -> None:
        super().__init__(caso, casos_anteriores)

    def __adequa_titulo_estudo(self, dadger: Dadger):
        nome_estudo = Configuracoes().nome_estudo
        ano = self.caso.ano
        mes = self.caso.mes
        rv = self.caso.revisao
        dadger.te.titulo = f"{nome_estudo} - DC {mes}/{ano} RV{rv}"

    def __adequa_caminho_fcf(self, dadger: Dadger, caso_cortes: CasoNEWAVE):
        # Verifica se é necessário e extrai os cortes
        sintetizador = SintetizadorNEWAVE(caso_cortes)
        if not sintetizador.verifica_cortes_extraidos():
            sintetizador.extrai_cortes()
        # Altera os registros FC
        arq = Arquivos.le_arquivo(caso_cortes.caminho)
        dadger.fc("NEWV21").caminho = join(caso_cortes.caminho, arq.cortesh)
        dadger.fc("NEWCUT").caminho = join(caso_cortes.caminho, arq.cortes)
        return True

    def __adequa_decks_decomp(self, dadger: Dadger):
        Log.log().info(f"Adequando caso do DECOMP: {self.caso.nome}")
        # Adequa registro NI
        n_iter = Configuracoes().maximo_iteracoes_decomp
        dadger.ni.iteracoes = n_iter
        # Adequa registro GP
        # TODO
        # Prevenção de Gap Negativo
        if Configuracoes().previne_gap_negativo:
            # Se não tem RT DESVIO, cria
            try:
                dadger.rt("DESVIO")
            except ValueError:
                rt = RT()
                rt.restricao = "DESVIO"
                dadger.cria_registro(dadger.te, rt)
            # Se não tem RT CRISTA, cria
            try:
                dadger.rt("CRISTA")
            except ValueError:
                rt = RT()
                rt.restricao = "CRISTA"
                dadger.cria_registro(dadger.te, rt)

    def prepara(self, **kwargs) -> bool:
        Log.log().info(f"Preparando caso do DECOMP: {self.caso.nome}")
        try:
            script = Configuracoes().script_converte_codificacao
            converte_codificacao(self.caso.caminho, script)
            dadger = Dadger.le_arquivo(
                self.caso.caminho, f"dadger.rv{self.caso.revisao}"
            )
            Log.log().info("Dadger lido com sucesso")
            # Adequa registro TE
            self.__adequa_titulo_estudo(dadger)
            # Adequa os registros FC (cortes e cortesh)
            caso_cortes = kwargs.get("caso_cortes")
            if caso_cortes is None or not isinstance(caso_cortes, CasoNEWAVE):
                Log.log().error("Erro na especificação dos cortes da FCF")
                raise RuntimeError()
            self.__adequa_caminho_fcf(dadger, caso_cortes)
            if Configuracoes().adequa_decks_decomp:
                self.__adequa_decks_decomp(dadger)
            # Salva o dadger
            dadger.escreve_arquivo(
                self.caso.caminho, f"dadger.rv{self.caso.revisao}"
            )
            Log.log().info("Adequação do caso concluída com sucesso")
            return True
        except FileNotFoundError as e:
            Log.log().error(f"Erro na leitura do dadger: {e}")
            return False

    def encadeia(self, casos_anteriores: List[Caso]) -> bool:
        def __decomps_anteriores():
            return [
                c
                for c in reversed(casos_anteriores)
                if isinstance(c, CasoDECOMP)
            ]

        if len(__decomps_anteriores()) == 0:
            Log.log().info(f"Primeiro: {self.caso.nome} - sem encadeamentos")
            return True
        elif isinstance(__decomps_anteriores()[0], CasoDECOMP):
            Log.log().info(
                "Encadeando variáveis dos casos "
                + f"{__decomps_anteriores()[0].nome}"
                + f" -> {self.caso.nome}"
            )
            encadeador = Encadeador.factory(casos_anteriores, self.caso)
            return encadeador.encadeia()
        else:
            Log.log().error(
                "Encadeamento NW com DC não suportado. Casos: "
                + f"{__decomps_anteriores()[0].nome}"
                + f" -> {self.caso.nome}"
            )
            return False

    def aplica_regras_operacao_reservatorios(
        self,
        casos_anteriores: List[Caso],
        regras_operacao: List[RegraReservatorio],
    ) -> bool:
        aplicador = AplicadorRegrasReservatorios.factory(self.caso)
        return aplicador.aplica_regras(casos_anteriores, regras_operacao)
