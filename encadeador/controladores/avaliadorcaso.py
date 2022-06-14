from abc import abstractmethod
import pandas as pd  # type: ignore
from typing import Optional
from inewave.newave import PMO  # type: ignore
from idecomp.decomp.sumario import Sumario
from idecomp.decomp.inviabunic import InviabUnic

from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.utils.log import Log


class AvaliadorCaso:
    def __init__(self, caso: Caso) -> None:
        self._caso = caso

    @staticmethod
    def factory(caso: Caso) -> "AvaliadorCaso":
        if isinstance(caso, CasoNEWAVE):
            return AvaliadorNEWAVE(caso)
        elif isinstance(caso, CasoDECOMP):
            return AvaliadorDECOMP(caso)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    @abstractmethod
    def avalia(self) -> bool:
        pass


class AvaliadorNEWAVE(AvaliadorCaso):
    def __init__(self, caso: CasoNEWAVE) -> None:
        super().__init__(caso)

    def __avalia_pmo(self) -> bool:
        try:
            pmo = PMO.le_arquivo(self._caso.caminho)
            return pmo.custo_operacao_series_simuladas is not None
        except (Exception, FileNotFoundError) as e:
            Log.log().error(
                "Erro na avaliação das saídas" + f" do {self._caso.nome}: {e}"
            )
            return False

    def avalia(self) -> bool:
        Log.log().info(f"Verificando saídas do {self._caso.nome}")
        sucesso_pmo = self.__avalia_pmo()
        if sucesso_pmo:
            Log.log().info(f"Caso concluído com sucesso: {self._caso.nome}")
        else:
            Log.log().error("Erro no processamento do " + f"{self._caso.nome}")
        return sucesso_pmo


class AvaliadorDECOMP(AvaliadorCaso):
    def __init__(self, caso: CasoDECOMP) -> None:
        super().__init__(caso)

    def __avalia_sumario(self) -> bool:
        try:
            arq = f"sumario.rv{self._caso.revisao}"
            sumario = Sumario.le_arquivo(self._caso.caminho, arq)
            return sumario.cmo_medio_subsistema is not None
        except (Exception, FileNotFoundError) as e:
            Log.log().error(
                "Erro na avaliação das saídas" + f" do {self._caso.nome}: {e}"
            )
            return False

    def __avalia_inviab(self) -> Optional[pd.DataFrame]:
        Log.log().info(f"Verificando inviabilidades do {self._caso.nome}")
        try:
            arq_inv = f"inviab_unic.rv{self._caso.revisao}"
            inviab = InviabUnic.le_arquivo(self._caso.caminho, arq_inv)
            return inviab.inviabilidades_simulacao_final
        except (Exception, FileNotFoundError) as e:
            Log.log().error(
                "Erro na avaliação das saídas" + f" do {self._caso.nome}: {e}"
            )
            return None

    def __avalia_inviab_primeiro_mes(
        self, inviabs_sim_final: pd.DataFrame
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
        return not all(
            [
                Configuracoes().flexibiliza_deficit,
                not inviabs_primeiro_mes.empty,
            ]
        )

    def avalia(self) -> bool:
        Log.log().info(f"Verificando saídas do {self._caso.nome}")
        if self.__avalia_sumario():
            inviabs_sim_final = self.__avalia_inviab()
            if inviabs_sim_final is None:
                Log.log().info(
                    f"Caso concluído com sucesso: {self._caso.nome}"
                )
                return True
            else:
                return self.__avalia_inviab_primeiro_mes(inviabs_sim_final)
        return False
