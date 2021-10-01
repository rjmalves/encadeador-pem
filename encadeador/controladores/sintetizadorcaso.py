from logging import Logger
from os.path import join, isdir, isfile
from os import makedirs, listdir, remove
from abc import abstractmethod
from typing import List
from zipfile import ZipFile
from logging import Logger

from inewave.newave import DeckEntrada, PMO  # type: ignore
from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP

DIRETORIO_RESUMO_CASO = "resumo"
PADRAO_ZIP_DECK_NEWAVE = "deck_"
PADRAO_ZIP_SAIDAS_NEWAVE = "saidas_"
PADRAO_ZIP_SAIDAS_NWLISTOP = "_out"


class SintetizadorCaso:

    def __init__(self,
                 caso: Caso,
                 log: Logger) -> None:
        self._caso = caso
        self._log = log
        # Cria o diretório de resumos se não existir
        self.__cria_diretorio_resumos()

    @staticmethod
    def factory(caso: Caso,
                log: Logger) -> 'SintetizadorCaso':
        if isinstance(caso, CasoNEWAVE):
            return SintetizadorNEWAVE(caso, log)
        elif isinstance(caso, CasoDECOMP):
            return SintetizadorDECOMP(caso, log)
        else:
            raise ValueError(f"Caso do tipo {type(caso)} não suportado")

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


class SintetizadorNEWAVE(SintetizadorCaso):

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

    def __procura_zip_saida(self) -> str:
        caminho = self.caso.caminho
        arqs_dir = listdir(caminho)
        padrao = PADRAO_ZIP_SAIDAS_NEWAVE
        try:
            arq_zip = [a for a in arqs_dir if padrao in a][0]
            if not isfile(join(caminho, arq_zip)):
                raise ValueError
            self._log.info(f"Encontrado arquivo com {padrao} em {caminho}")
            return join(caminho, arq_zip)
        except Exception as e:
            self._log.error(f"Nao foi encontrado um zip com {padrao}")
            raise e

    @property
    def _nomes_arquivos_cortes(self) -> List[str]:
        deck = DeckEntrada.le_deck(self.caso.caminho)
        arq_cortes = deck.arquivos.cortes
        arq_cortesh = deck.arquivos.cortesh
        return [arq_cortes, arq_cortesh]

    def extrai_cortes(self):
        arq_zip = self.__procura_zip_saida()
        try:
            with ZipFile(arq_zip, "r") as obj_zip:
                arqs = obj_zip.namelist()
                for a in self._nomes_arquivos_cortes:
                    if a not in arqs:
                        raise ValueError(a)
                    self._log.info(f"Extraindo {a} de {arq_zip}...")
                    obj_zip.extract(a, self.caso.caminho)
        except ValueError as v:
            self._log.error(f"Não foi encontrado o arquivo {str(v)}")

    def deleta_cortes(self):
        for a in self._nomes_arquivos_cortes:
            caminho = join(self.caso.caminho, a)
            if isfile(caminho):
                remove(caminho)
                self._log.info(f"Arquivo {caminho} deletado")
            else:
                raise ValueError(f"Arquivo {caminho} não deletado")

    def verifica_cortes_extraidos(self) -> bool:
        arqs_dir = listdir(self.caso.caminho)
        cortes_extraidos = [c in arqs_dir for c in
                            self._nomes_arquivos_cortes]
        return all(cortes_extraidos)


class SintetizadorDECOMP(SintetizadorCaso):

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
