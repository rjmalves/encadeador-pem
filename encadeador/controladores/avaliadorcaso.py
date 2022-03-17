from abc import abstractmethod
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

    def avalia(self) -> bool:
        try:
            Log.log().info(f"Verificando saídas do {self._caso.nome}")
            pmo = PMO.le_arquivo(self._caso.caminho)
            custos = pmo.custo_operacao_series_simuladas
            if custos.empty:
                Log.log().error(
                    "Erro no processamento do " + f"{self._caso.nome}"
                )
                return False
            Log.log().info(f"Caso concluído com sucesso: {self._caso.nome}")
            return True
        except FileNotFoundError:
            Log.log().error(
                "Arquivo pmo.dat não encontrado"
                + f" no diretório do {self._caso.nome}"
            )
            raise RuntimeError()
        except Exception as e:
            Log.log().error(
                "Erro na avaliação das saídas" + f" do {self._caso.nome}: {e}"
            )
            return False


class AvaliadorDECOMP(AvaliadorCaso):
    def __init__(self, caso: CasoDECOMP) -> None:
        super().__init__(caso)

    def avalia(self) -> bool:
        try:
            arq = f"sumario.rv{self._caso.revisao}"
            arq_inv = f"inviab_unic.rv{self._caso.revisao}"
            Log.log().info(f"Verificando saídas do {self._caso.nome}")
            sumario = Sumario.le_arquivo(self._caso.caminho, arq)
            sumario.cmo_medio_subsistema
            Log.log().info(f"Verificando inviabilidades do {self._caso.nome}")
            inviab = InviabUnic.le_arquivo(self._caso.caminho, arq_inv)
            if not inviab.inviabilidades_simulacao_final.empty:
                Log.log().warning(
                    f"{self._caso.nome} convergiu com"
                    + " inviabilidades na simulação final"
                )
                cmo = sumario.cmo_medio_subsistema
                n_estagios = (
                    len([c for c in list(cmo.columns) if "Estágio" in c]) + 1
                )
                invs = inviab.inviabilidades_simulacao_final
                inviabs_primeiro_mes = invs.loc[
                    invs["Estágio"] != n_estagios, :
                ]
                if (
                    Configuracoes().flexibiliza_deficit
                    and not inviabs_primeiro_mes.empty
                ):
                    return False
            Log.log().info(f"Caso concluído com sucesso: {self._caso.nome}")
            return True
        except FileNotFoundError:
            Log.log().error(
                f"Arquivo {arq} ou {arq_inv} não encontrados"
                + f" no diretório do {self._caso.nome}"
            )
            raise RuntimeError()
        except Exception as e:
            Log.log().warning(
                "Erro na avaliação das saídas"
                + f" do {self._caso.nome}: caso não convergiu: {e}"
            )
            return False
