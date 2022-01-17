from os import listdir, sep
from typing import List, Optional
from os.path import isdir, join, normpath

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.utils.log import Log


# TODO - renomear para classe Estudo
class ArvoreCasos:

    def __init__(self) -> None:
        self._diretorios_revisoes: List[str] = []
        self._diretorios_casos: List[str] = []
        self._casos: List[Caso] = []

    def __verifica_inicializacao(self, valor):
        if len(valor) == 0:
            raise ValueError("ArvoreCasos não inicializada!")

    def le_arquivo_casos(self):

        def __le_diretorios():
            for d in self._diretorios_revisoes:
                subd = [a for a in listdir(d) if isdir(join(d, a))]
                if Configuracoes().nome_diretorio_newave in subd:
                    c = join(Configuracoes().caminho_base_estudo,
                             d,
                             Configuracoes()._nome_diretorio_newave)
                    self._diretorios_casos.append(c)
                if Configuracoes()._nome_diretorio_decomp in subd:
                    c = join(Configuracoes().caminho_base_estudo,
                             d,
                             Configuracoes()._nome_diretorio_decomp)
                    self._diretorios_casos.append(c)

        lista = Configuracoes().arquivo_lista_casos
        with open(lista, "r") as arq:
            dirs = arq.readlines()
        dirs = [c.strip("\n").strip() for c in dirs]
        self._diretorios_revisoes = dirs
        __le_diretorios()

    def constroi_casos(self) -> bool:

        def __le_caso(c: str) -> bool:
            pastas = normpath(c).split(sep)
            # Extrai as características do caso
            diretorio_caso = pastas[-2]
            componentes_caso = diretorio_caso.split("_")
            ano = int(componentes_caso[0])
            mes = int(componentes_caso[1])
            rv = int(componentes_caso[2].split("rv")[1])
            # Identifica o programa
            diretorio_prog = pastas[-1]
            if Configuracoes()._nome_diretorio_newave == diretorio_prog:
                caso_nw = CasoNEWAVE()
                caso_nw.configura_caso(c, ano, mes, rv)
                self._casos.append(caso_nw)
                return True
            elif Configuracoes()._nome_diretorio_decomp == diretorio_prog:
                caso_dcp = CasoDECOMP()
                caso_dcp.configura_caso(c, ano, mes, rv)
                self._casos.append(caso_dcp)
                return True
            else:
                Log.log().error(f"Diretório inválido: {diretorio_prog}")
                return False

        for c in self._diretorios_casos:
            try:
                caso = ArmazenadorCaso.recupera_caso(c)
                # TODO - Pensar em como permitir mudanças de diretório
                # do estudo encadeado, uma vez já concluído.
                caso.caminho = c
                self._casos.append(caso)
            except FileNotFoundError:
                ret = __le_caso(c)
                if not ret:
                    return False

        return True

    @property
    def casos(self) -> List[Caso]:
        self.__verifica_inicializacao(self._casos)
        return self._casos

    def indice_caso(self, caso: Optional[Caso]) -> int:
        if caso is None:
            return 0
        else:
            return self._casos.index(caso)

    @property
    def proximo_caso(self) -> Optional[Caso]:
        for c in self.casos:
            try:
                if c.estado != EstadoCaso.CONCLUIDO:
                    return c
            except ValueError:
                return c
        return None

    @property
    def proximo_newave(self) -> Optional[CasoNEWAVE]:
        for c in self.casos:
            try:
                if all([isinstance(c, CasoNEWAVE),
                        c.estado != EstadoCaso.CONCLUIDO]):
                    return c
            except ValueError:
                if isinstance(c, CasoNEWAVE):
                    return c
        return None

    @property
    def proximo_decomp(self) -> Optional[CasoDECOMP]:
        for c in self.casos:
            try:
                if all([isinstance(c, CasoDECOMP),
                        c.estado != EstadoCaso.CONCLUIDO]):
                    return c
            except ValueError:
                if isinstance(c, CasoDECOMP):
                    return c
        return None

    @property
    def ultimo_caso(self) -> Optional[Caso]:
        c_convergido = None
        for c in self.casos:
            try:
                if c.estado == EstadoCaso.CONCLUIDO:
                    c_convergido = c
            except ValueError:
                break
        return c_convergido

    @property
    def ultimo_newave(self) -> Optional[CasoNEWAVE]:
        c_convergido = None
        for c in self.casos:
            try:
                if all([isinstance(c, CasoNEWAVE),
                        c.estado == EstadoCaso.CONCLUIDO]):
                    c_convergido = c
            except ValueError:
                break
        return c_convergido

    @property
    def ultimo_decomp(self) -> Optional[CasoDECOMP]:
        c_convergido = None
        for c in self.casos:
            try:
                if all([isinstance(c, CasoDECOMP),
                        c.estado == EstadoCaso.CONCLUIDO]):
                    c_convergido = c
            except ValueError:
                break
        return c_convergido

    @property
    def casos_concluidos(self) -> List[Caso]:
        casos: List[Caso] = []
        for c in self.casos:
            try:
                if c.estado == EstadoCaso.CONCLUIDO:
                    casos.append(c)
            except ValueError:
                break
        return casos

    @property
    def terminou(self) -> bool:
        sucesso: List[bool] = []
        try:
            for c in self.casos:
                sucesso.append(c.estado == EstadoCaso.CONCLUIDO)
        except ValueError:
            return False
        return all(sucesso)
