from typing import List
from logging import Logger
from os import listdir, sep
from os.path import isdir, join, normpath

from encadeador.modelos.caso import Configuracoes
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP


class ArvoreCasos:

    def __init__(self,
                 cfg: Configuracoes,
                 log: Logger) -> None:
        self._log = log
        self._configuracoes = cfg
        self._diretorios_revisoes: List[str] = []
        self._diretorios_casos: List[str] = []
        self._casos: List[Caso] = []

    def le_arquivo_casos(self):

        def __le_diretorios():
            for d in self._diretorios_revisoes:
                subd = [a for a in listdir(d) if isdir(a)]
                if self._configuracoes.diretorio_newave in subd:
                    c = join(d, self._configuracoes.diretorio_newave)
                    self._diretorios_casos.append(c)
                if self._configuracoes.diretorio_decomp in subd:
                    c = join(d, self._configuracoes.diretorio_decomp)
                    self._diretorios_casos.append(c)

        lista = self._configuracoes.arquivo_lista_casos
        with open(lista, "r") as arq:
            dirs = arq.readlines()
        dirs = [c.strip("\n").strip() for c in dirs]
        self._diretorios_revisoes = dirs
        __le_diretorios()

    def constroi_casos(self) -> bool:

        def __le_caso():
            pass

        for c in self._diretorios_casos:
            try:
                caso = ArmazenadorCaso.recupera_caso(self._configuracoes,
                                                     c)
                self._casos.append(caso)
            except FileNotFoundError as e:
                pastas = normpath(c).split(sep)
                # Extrai as características do caso
                diretorio_caso = pastas[-2]
                componentes_caso = diretorio_caso.split("_")
                ano = int(componentes_caso[0])
                mes = int(componentes_caso[1])
                rv = int(componentes_caso[2].split("rv")[1])
                # Identifica o programa
                diretorio_prog = pastas[-1]
                if self._configuracoes.diretorio_newave == diretorio_prog:
                    caso = CasoNEWAVE()
                    caso.configura_caso(c, )
                    self._casos.append()
                elif self._configuracoes.diretorio_decomp == diretorio_prog:
                    self._casos.append()
                else:
                    self._log.error(f"Diretório inválido: {diretorio_prog}")
                    return False
        return True
