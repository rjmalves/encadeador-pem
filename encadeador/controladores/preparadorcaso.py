


from abc import abstractmethod
from encadeador.modelos.caso import Caso, CasoNEWAVE

class PreparadorCaso:

    def __init__(self,
                 caso: Caso) -> None:
        self._caso = caso

    @abstractmethod
    def prepara_caso(self) -> bool:
        pass


class PreparadorCasoNEWAVE(PreparadorCaso):

    def __init__(self, caso: CasoNEWAVE) -> None:
        super().__init__(caso)

    def prepara_caso(self) -> bool:
        pass
