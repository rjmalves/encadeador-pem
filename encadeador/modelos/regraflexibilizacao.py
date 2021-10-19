from abc import abstractmethod
from logging import Logger
from idecomp.decomp.dadger import Dadger  # type: ignore

from encadeador.modelos.inviabilidade import Inviabilidade
from encadeador.modelos.inviabilidade import InviabilidadeEV
from encadeador.modelos.inviabilidade import InviabilidadeTI
from encadeador.modelos.inviabilidade import InviabilidadeHV
from encadeador.modelos.inviabilidade import InviabilidadeHQ
from encadeador.modelos.inviabilidade import InviabilidadeRE
from encadeador.modelos.inviabilidade import InviabilidadeHE
from encadeador.modelos.inviabilidade import InviabilidadeDeficit


class RegraFlexibilizacao:

    def __init__(self,
                 log: Logger) -> None:
        self._log = log

    @staticmethod
    def factory(metodo: str,
                log: Logger) -> 'RegraFlexibilizacao':
        if metodo == "absoluto":
            return RegraFlexibilizacaoAbsoluto(log)
        elif metodo == "percentual":
            return RegraFlexibilizacaoPercentual(log)
        else:
            raise ValueError(f"Regra de flexibilização {metodo} não suportada")

    @abstractmethod
    def _flexibilizaEV(self,
                       dadger: Dadger,
                       inviabilidade: InviabilidadeEV):
        pass

    @abstractmethod
    def _flexibilizaTI(self,
                       dadger: Dadger,
                       inviabilidade: InviabilidadeTI):
        pass

    @abstractmethod
    def _flexibilizaHV(self,
                       dadger: Dadger,
                       inviabilidade: InviabilidadeHV):
        pass

    @abstractmethod
    def _flexibilizaHQ(self,
                       dadger: Dadger,
                       inviabilidade: InviabilidadeHQ):
        pass

    @abstractmethod
    def _flexibilizaRE(self,
                       dadger: Dadger,
                       inviabilidade: InviabilidadeRE):
        pass

    @abstractmethod
    def _flexibilizaHE(self,
                       dadger: Dadger,
                       inviabilidade: InviabilidadeHE):
        pass

    @abstractmethod
    def _flexibiliza_deficit(self,
                             dadger: Dadger,
                             inviabilidade: InviabilidadeDeficit):
        pass

    def flexibiliza(self,
                    dadger: Dadger,
                    inviabilidade: Inviabilidade):

        if isinstance(inviabilidade, InviabilidadeEV):
            self._flexibilizaEV(dadger, inviabilidade)
        elif isinstance(inviabilidade, InviabilidadeTI):
            self._flexibilizaTI(dadger, inviabilidade)
        elif isinstance(inviabilidade, InviabilidadeHV):
            self._flexibilizaHV(dadger, inviabilidade)
        elif isinstance(inviabilidade, InviabilidadeHQ):
            self._flexibilizaHQ(dadger, inviabilidade)
        elif isinstance(inviabilidade, InviabilidadeRE):
            self._flexibilizaRE(dadger, inviabilidade)
        elif isinstance(inviabilidade, InviabilidadeHE):
            self._flexibilizaHE(dadger, inviabilidade)
        elif isinstance(inviabilidade, InviabilidadeDeficit):
            self._flexibiliza_deficit(dadger, inviabilidade)
        else:
            raise ValueError(f"Inviabilidade {type(inviabilidade)}" +
                             " não suportada")


class RegraFlexibilizacaoAbsoluto(RegraFlexibilizacao):

    def __init__(self,
                 log: Logger) -> None:
        super().__init__(log)

    # Override
    def flexibiliza(self,
                    dadger: Dadger,
                    inviabilidade: Inviabilidade):
        pass


class RegraFlexibilizacaoPercentual(RegraFlexibilizacao):

    def __init__(self,
                 log: Logger) -> None:
        super().__init__(log)
