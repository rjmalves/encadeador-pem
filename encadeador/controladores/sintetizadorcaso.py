from logging import Logger
from os.path import join, isdir
from os import makedirs
from abc import abstractmethod

from inewave.newave import PMO
from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP

DIRETORIO_RESUMO_CASO = "resumo"


class SintetizadorCaso:

    def __init__(self,
                 caso: Caso,
                 log: Logger) -> None:
        self._caso = caso
        self._log = log
        # Cria o diretório de resumos se não existir
        self.__cria_diretorio_resumos()

    def __cria_diretorio_resumos(self):
        diretorio = join(self.caso.caminho,
                         DIRETORIO_RESUMO_CASO)
        if not isdir(diretorio):
            makedirs(diretorio)

    @abstractmethod
    def sintetiza_caso(self) -> bool:
        pass

    @property
    def caso(self) -> Caso:
        return self._caso


class SintetizadorCasoNEWAVE(SintetizadorCaso):

    def __init__(self,
                 caso: CasoNEWAVE,
                 log: Logger):
        super().__init__(caso, log)

    def sintetiza_caso(self) -> bool:
        try:
            pmo = PMO.le_arquivo(self.caso.caminho)
            caminho_saida = join(self.caso.caminho,
                                 DIRETORIO_RESUMO_CASO)
            # Convergência do pmo.dat
            pmo.convergencia.to_csv(join(caminho_saida,
                                         "convergencia.csv"),
                                    header=True,
                                    encoding="utf-8")
            # Custos do pmo.dat
            pmo.custo_operacao_series_simuladas.to_csv(join(caminho_saida,
                                                            "custos.csv"),
                                                       header=True,
                                                       encoding="utf-8")
            # CMO, EARM, GT, GH do NWLISTOP
            # TODO
            return True
        except Exception as e:
            self._log.error(f"Erro de síntese do caso {self.caso.nome}: {e}")
            return False


class SintetizadorCasoDECOMP(SintetizadorCaso):

    def __init__(self,
                 caso: CasoDECOMP,
                 log: Logger):
        super().__init__(caso, log)

    def sintetiza_caso(self) -> bool:
        try:
            # Convergência do relato.rvX
            # TODO
            # Inviabilidades do inviab_unic.rvX
            # TODO
            # CMO, EARM, GT, GH, Déficit do relato.rvX
            # TODO
            return True
        except Exception as e:
            self._log.error(f"Erro de síntese do caso {self.caso.nome}: {e}")
            return False
