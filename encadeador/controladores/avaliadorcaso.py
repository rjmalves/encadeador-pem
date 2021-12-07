from abc import abstractmethod
from logging import Logger
from inewave.newave import PMO  # type: ignore
from idecomp.decomp.sumario import Sumario
from idecomp.decomp.inviabunic import InviabUnic

from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE


class AvaliadorCaso:

    def __init__(self,
                 caso: Caso,
                 log: Logger) -> None:
        self._caso = caso
        self._log = log

    @staticmethod
    def factory(caso: Caso,
                log: Logger) -> 'AvaliadorCaso':
        if isinstance(caso, CasoNEWAVE):
            return AvaliadorNEWAVE(caso, log)
        elif isinstance(caso, CasoDECOMP):
            return AvaliadorDECOMP(caso, log)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

    @abstractmethod
    def avalia(self) -> bool:
        pass


class AvaliadorNEWAVE(AvaliadorCaso):

    def __init__(self,
                 caso: CasoNEWAVE,
                 log: Logger) -> None:
        super().__init__(caso,
                         log)

    def avalia(self) -> bool:
        try:
            self._log.info(f"Verificando saídas do {self._caso.nome}")
            pmo = PMO.le_arquivo(self._caso.caminho)
            custos = pmo.custo_operacao_series_simuladas
            if custos.empty:
                self._log.error("Erro no processamento do " +
                                f"{self._caso.nome}")
                return False
            self._log.info(f"Caso concluído com sucesso: {self._caso.nome}")
            return True
        except FileNotFoundError:
            self._log.error("Arquivo pmo.dat não encontrado" +
                            f" no diretório do {self._caso.nome}")
            raise RuntimeError()
        except Exception as e:
            self._log.error("Erro na avaliação das saídas" +
                            f" do {self._caso.nome}: {e}")
            return False


class AvaliadorDECOMP(AvaliadorCaso):

    def __init__(self,
                 caso: CasoDECOMP,
                 log: Logger) -> None:
        super().__init__(caso,
                         log)

    def avalia(self) -> bool:
        try:
            arq = f"sumario.rv{self._caso.revisao}"
            arq_inv = f"inviab_unic.rv{self._caso.revisao}"
            self._log.info(f"Verificando saídas do {self._caso.nome}")
            sumario = Sumario.le_arquivo(self._caso.caminho, arq)
            sumario.cmo_medio_subsistema
            self._log.info(f"Verificando inviabilidades do {self._caso.nome}")
            inviab = InviabUnic.le_arquivo(self._caso.caminho, arq_inv)
            if not inviab.inviabilidades_simulacao_final.empty:
                self._log.warning(f"{self._caso.nome} convergiu com" +
                                  " inviabilidades na simulação final")
                if self._caso.configuracoes.flexibiliza_deficit:
                    return False
            self._log.info(f"Caso concluído com sucesso: {self._caso.nome}")
            return True
        except FileNotFoundError:
            self._log.error(f"Arquivo {arq} ou {arq_inv} não encontrados" +
                            f" no diretório do {self._caso.nome}")
            raise RuntimeError()
        except Exception:
            self._log.warning("Erro na avaliação das saídas" +
                              f" do {self._caso.nome}: caso não convergiu.")
            return False
