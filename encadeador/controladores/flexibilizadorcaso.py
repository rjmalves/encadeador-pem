from abc import abstractmethod
from logging import Logger
from idecomp.decomp.inviabunic import InviabUnic  # type: ignore
from idecomp.decomp.dadger import Dadger  # type: ignore

from encadeador.modelos.caso import Caso, CasoDECOMP


class Flexibilizador:

    def __init__(self,
                 caso: Caso,
                 log: Logger):
        self._caso = caso
        self._log = log

    @staticmethod
    def factory(caso: Caso,
                log: Logger) -> 'Flexibilizador':
        if isinstance(caso, CasoDECOMP):
            return FlexibilizadorDECOMP(caso,
                                        log)
        else:
            raise TypeError(f"Caso do tipo {type(caso)} " +
                            "não suportado para encadeamento")

    @abstractmethod
    def flexibiliza(self) -> bool:
        pass


class FlexibilizadorDECOMP(Flexibilizador):

    def __init__(self,
                 caso: Caso,
                 log: Logger):
        super().__init__(caso, log)

    def flexibiliza(self) -> bool:
        max_flex = self._caso.configuracoes.maximo_flexibilizacoes_revisao
        self._log.info(f"Flexibilizando caso {self._caso.nome}: " +
                       f"{self._caso.numero_flexibilizacoes} de {max_flex}")
        self._caso.adiciona_flexibilizacao()
        # Lê o inviab_unic.rvX
        arq_inviab = f"inviab_unic.rv{self._caso.revisao}"
        inviab = InviabUnic.le_arquivo(self._caso.caminho, arq_inviab)
        # Lê o dadger.rvX
        arq_dadger = f"dadger.rv{self._caso.revisao}"
        dadger = Dadger.le_arquivo(self._caso.caminho, arq_dadger)
        # TODO - Faz as flexibilizações
        # Escreve o dadger.rvX de saída
        dadger.escreve_arquivo(self._caso.caminho, arq_dadger)
        return True
