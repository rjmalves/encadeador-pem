from abc import abstractmethod
from logging import Logger
from inewave.newave import PMO

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
            self._log.info(f"Verificando saídas do NW {self._caso.nome}")
            pmo = PMO.le_arquivo(self._caso.caminho)
            custos = pmo.custo_operacao_series_simuladas
            if custos.empty:
                self._log.error("Erro no processamento do NW " +
                                f"{self._caso.nome}")
                return False
            self._log.info(f"Caso concluído com sucesso: {self._caso.nome}")
            return True
        except FileNotFoundError:
            self._log.error("Arquivo pmo.dat não encontrado" +
                            f" no diretório do NW {self._caso.nome}")
            return False
        except Exception:
            return False


class AvaliadorDECOMP(AvaliadorCaso):

    def __init__(self,
                 caso: CasoDECOMP,
                 log: Logger) -> None:
        super().__init__(caso,
                         log)

    def avalia(self) -> bool:
        # TODO - Conferir se existe algum dado no sumario.rvX
        return True
