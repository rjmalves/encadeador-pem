from abc import abstractmethod
from os.path import join
from typing import List, Optional

from encadeador.modelos.caso import Caso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.programa import Programa

from encadeador.services.unitofwork.newave import factory as nw_factory
from encadeador.services.unitofwork.decomp import factory as dc_factory
from encadeador.domain.programs import ProgramRules
from encadeador.utils.log import Log
from inewave.newave import DGer, CVAR  # type: ignore
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.modelos.dadger import RT, FC


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
            raise ValueError("Caso não suportado")

    @abstractmethod
    async def prepara(self) -> bool:
        pass

    @abstractmethod
    async def corrige_erro_convergencia(self) -> bool:
        pass

    @abstractmethod
    async def flexibiliza_criterio_convergencia(self) -> bool:
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
                uow = nw_factory("FS", c.caminho)
                with uow:
                    Log.log().info(
                        "Deletando cortes do último NEWAVE: " + f"{c.caminho}"
                    )
                    uow.deleta_cortes()

    def __adequa_dger(self, dger: DGer):
        ano = self.caso.ano
        mes = self.caso.mes
        dger.nome_caso = ProgramRules.newave_case_name(ano, mes)

    def __adequa_cvar(self, cvar: CVAR):
        par_cvar = Configuracoes().cvar
        cvar.valores_constantes = par_cvar
        Log.log().info(f"Valores de CVAR alterados: {par_cvar}")

    async def prepara(self) -> bool:
        Log.log().info(f"Preparando caso do NEWAVE: {self.caso.nome}")
        self.__deleta_cortes_ultimo_newave()
        uow = nw_factory(
            "FS", join(Configuracoes().caminho_base_estudo, self.caso.caminho)
        )
        with uow:
            if Configuracoes().adequa_decks_newave:
                dger = await uow.newave.get_dger()
                self.__adequa_dger(dger)
                uow.newave.set_dger(dger)
                cvar = uow.newave.get_cvar()
                self.__adequa_cvar(cvar)
                uow.newave.set_cvar(cvar)
            Log.log().info("Adequação do caso concluída com sucesso")
            return True

    async def corrige_erro_convergencia(self) -> bool:
        Log.log().info(
            "Não há correção de erro de convergência no NEWAVE: "
            + f"{self.caso.nome}"
        )
        return True

    async def flexibiliza_criterio_convergencia(self) -> bool:
        Log.log().info(
            "Não há flexibilização de critério de convergência no NEWAVE: "
            + f"{self.caso.nome}"
        )
        return True


class PreparadorDECOMP(PreparadorCaso):
    def __init__(self, caso: Caso, casos_anteriores: List[Caso]) -> None:
        super().__init__(caso, casos_anteriores)

    def __ultimo_newave(self) -> Optional[Caso]:
        for c in reversed(self._casos_anteriores):
            if c.programa == Programa.NEWAVE:
                return c
        return None

    def __extrai_cortes_ultimo_newave(self, c: Optional[Caso]):
        if c is not None:
            uow = nw_factory("FS", c.caminho)
            with uow:
                Log.log().info(
                    "Extraindo cortes do último NEWAVE: " + f"{c.caminho}"
                )
                uow.extrai_cortes()

    def __adequa_caminho_fcf(self, dadger: Dadger, caso_cortes: Caso):
        # Verifica se é necessário e extrai os cortes
        self.__extrai_cortes_ultimo_newave(caso_cortes)
        # Altera os registros FC
        nw_uow = nw_factory("FS", caso_cortes.caminho)
        with nw_uow:
            arq = nw_uow.newave.arquivos
            arq_cortes = arq.cortes
            arq_cortesh = arq.cortesh
        if arq_cortesh is None:
            Log.log().error(
                "Nome do arquivo de cabeçalho de cortes não especificado"
            )
            return False
        if arq_cortes is None:
            Log.log().error("Nome do arquivo de cortes não especificado")
            return False
        fc_cortesh = dadger.fc("NEWV21")
        if not isinstance(fc_cortesh, FC):
            Log.log().error("Caso não possui registro FC NEWV21")
            return False
        else:
            fc_cortesh.caminho = join(caso_cortes.caminho, arq_cortesh)
        fc_cortes = dadger.fc("NEWCUT")
        if not isinstance(fc_cortes, FC):
            Log.log().error("Caso não possui registro FC NEWCUT")
            return False
        else:
            fc_cortes.caminho = join(caso_cortes.caminho, arq_cortes)
        return True

    def __adequa_titulo_estudo(self, dadger: Dadger):
        ano = self.caso.ano
        mes = self.caso.mes
        rv = self.caso.revisao
        reg_te = dadger.te
        if reg_te is None:
            Log.log().warning("Caso não possui registro TE")
        else:
            reg_te.titulo = ProgramRules.decomp_case_name(ano, mes, rv)

    def __adequa_numero_iteracoes(self, dadger: Dadger):
        reg_ni = dadger.ni
        if reg_ni is None:
            Log.log().warning("Caso não possui registro NI")
        else:
            reg_ni.iteracoes = Configuracoes().maximo_iteracoes_decomp

    def __adequa_dadger(self, dadger: Dadger):
        Log.log().info(f"Adequando caso do DECOMP: {self.caso.nome}")
        self.__adequa_titulo_estudo(dadger)
        self.__adequa_numero_iteracoes(dadger)

    async def prepara(self) -> bool:
        Log.log().info(f"Preparando caso do DECOMP: {self.caso.nome}")
        dc_uow = dc_factory(
            "FS", join(Configuracoes().caminho_base_estudo, self.caso.caminho)
        )
        with dc_uow:
            dadger = await dc_uow.decomp.get_dadger()
            # Adequa os registros FC (cortes e cortesh)
            caso_cortes = self.__ultimo_newave()
            if (
                caso_cortes is None
                or not caso_cortes.programa == Programa.NEWAVE
            ):
                Log.log().error("Erro na especificação dos cortes da FCF")
                return False
            self.__adequa_caminho_fcf(dadger, caso_cortes)
            if Configuracoes().adequa_decks_decomp:
                self.__adequa_dadger(dadger)

            dc_uow.decomp.set_dadger(dadger)
            Log.log().info("Adequação do caso concluída com sucesso")
        return True

    async def corrige_erro_convergencia(self) -> bool:
        Log.log().info(f"Previnindo gap negativo no DECOMP: {self.caso.nome}")
        dc_uow = dc_factory(
            "FS", join(Configuracoes().caminho_base_estudo, self.caso.caminho)
        )
        with dc_uow:
            dadger = await dc_uow.decomp.get_dadger()
            existe_rt_crista = dadger.rt(restricao="CRISTA") is not None
            existe_rt_desvio = dadger.rt(restricao="DESVIO") is not None
            if existe_rt_crista and existe_rt_desvio:
                Log.log().error(
                    "Restrições de soleira já removidas no DECOMP:"
                    + f" {self.caso.nome}"
                )
                return False
            reg_te = dadger.te
            if reg_te is None:
                Log.log().error(
                    "Não existe registro TE no caso:" + f" {self.caso.nome}"
                )
                return False
            if not existe_rt_crista:
                rt = RT()
                rt.restricao = "CRISTA"
                dadger.cria_registro(reg_te, rt)
            if not existe_rt_desvio:
                rt = RT()
                rt.restricao = "DESVIO"
                dadger.cria_registro(reg_te, rt)
            dc_uow.decomp.set_dadger(dadger)
        return True

    async def flexibiliza_criterio_convergencia(self) -> bool:
        Log.log().info(f"Aumentando gap do DECOMP: {self.caso.nome}")
        dc_uow = dc_factory(
            "FS",
            join(Configuracoes().caminho_base_estudo, self.caso.caminho),
        )
        with dc_uow:
            dadger = await dc_uow.decomp.get_dadger()
            reg_gp = dadger.gp
            if reg_gp is None:
                Log.log().error(
                    f"Não encontrado registro GP: {self.caso.nome}"
                )
                return False
            gap_atual = reg_gp.gap
            if gap_atual is None:
                Log.log().error(
                    f"Registro GP não possui gap configurado: {self.caso.nome}"
                )
                return False
            if gap_atual >= Configuracoes().gap_maximo_decomp:
                Log.log().error(
                    f"Máximo gap atingido no DECOMP: {self.caso.nome}"
                )
                return False
            reg_gp.gap = 10 * gap_atual
            dc_uow.decomp.set_dadger(dadger)
        return True
