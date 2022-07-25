from abc import abstractmethod
import pandas as pd  # type: ignore
from typing import Optional
from cfinterface.components.defaultblock import DefaultBlock
from encadeador.modelos.transicaocaso import TransicaoCaso
from encadeador.services.unitofwork.newave import factory as nw_uow_factory
from encadeador.services.unitofwork.decomp import factory as dc_uow_factory

from encadeador.modelos.caso2 import Caso
from encadeador.modelos.programa import Programa
from encadeador.utils.log import Log


class AvaliadorCaso:
    def __init__(self, caso: Caso) -> None:
        self._caso = caso

    @staticmethod
    def factory(caso: Caso) -> "AvaliadorCaso":
        if caso.programa == Programa.NEWAVE:
            return AvaliadorNEWAVE(caso)
        elif caso.programa == Programa.DECOMP:
            return AvaliadorDECOMP(caso)

    @abstractmethod
    def avalia(self) -> TransicaoCaso:
        pass


class AvaliadorNEWAVE(AvaliadorCaso):
    def __init__(self, caso: Caso) -> None:
        super().__init__(caso)
        self._uow = nw_uow_factory("FS", caso.caminho)

    def __avalia_pmo(self) -> bool:
        with self._uow:
            pmo = self._uow.newave.get_pmo()
            return pmo.custo_operacao_series_simuladas is not None

    def avalia(self) -> TransicaoCaso:
        Log.log().info(f"Verificando saídas do {self._caso.nome}")
        sucesso_pmo = self.__avalia_pmo()
        if sucesso_pmo:
            Log.log().info(f"Caso concluído com sucesso: {self._caso.nome}")
            return TransicaoCaso.CONCLUIDO
        else:
            Log.log().error("Erro no processamento do " + f"{self._caso.nome}")
            return TransicaoCaso.ERRO_DADOS


class AvaliadorDECOMP(AvaliadorCaso):

    PADRAO_ERRO_DADOS = "ERRO(S) DE ENTRADA DE DADOS"

    def __init__(self, caso: Caso) -> None:
        super().__init__(caso)
        self._uow = dc_uow_factory("FS", caso.caminho)

    def __avalia_erro_dados(self) -> bool:
        try:
            with self._uow:
                relato = self._uow.decomp.get_relato()
                return any(
                    [
                        AvaliadorDECOMP.PADRAO_ERRO_DADOS in b.data
                        for b in relato.data.of_type(DefaultBlock)
                    ]
                )
        except Exception as e:
            Log.log().error(
                "Erro na avaliação das saídas" + f" do {self._caso.nome}: {e}"
            )
            return False

    def __avalia_relato(self) -> bool:
        try:
            with self._uow:
                return (
                    self._uow.decomp.get_relato().cmo_medio_subsistema
                    is not None
                )
        except Exception as e:
            Log.log().error(
                "Erro na avaliação das saídas" + f" do {self._caso.nome}: {e}"
            )
            return False

    def __avalia_inviab(self) -> Optional[pd.DataFrame]:
        Log.log().info(f"Verificando inviabilidades do {self._caso.nome}")
        try:
            with self._uow:
                return (
                    self._uow.decomp.get_inviab().inviabilidades_simulacao_final
                )
        except Exception as e:
            Log.log().error(
                "Erro na avaliação das saídas" + f" do {self._caso.nome}: {e}"
            )
            return None

    def __avalia_inviab_primeiro_mes(
        self, inviabs_sim_final: pd.DataFrame, flex_deficit: bool = False
    ) -> bool:
        inviabs_primeiro_mes = pd.DataFrame()
        if not inviabs_sim_final.empty:
            Log.log().warning(
                f"{self._caso.nome} convergiu com"
                + " inviabilidades na simulação final"
            )
            ultimo_estagio = sorted(
                inviabs_sim_final["Estágio"].unique().tolist()
            )[-1]
            inviabs_primeiro_mes = inviabs_sim_final.loc[
                inviabs_sim_final["Estágio"] != ultimo_estagio, :
            ]
        if flex_deficit and not inviabs_primeiro_mes.empty:
            return TransicaoCaso.INVIAVEL
        else:
            return TransicaoCaso.CONCLUIDO

    def avalia(self) -> TransicaoCaso:
        Log.log().info(f"Verificando saídas do {self._caso.nome}")
        if self.__avalia_erro_dados():
            return TransicaoCaso.ERRO_DADOS
        if self.__avalia_relato():
            inviabs_sim_final = self.__avalia_inviab()
            if inviabs_sim_final is None:
                Log.log().info(
                    f"Caso concluído com sucesso: {self._caso.nome}"
                )
                return TransicaoCaso.CONCLUIDO
            else:
                return self.__avalia_inviab_primeiro_mes(inviabs_sim_final)
        return TransicaoCaso.INVIAVEL
