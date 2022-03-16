from os import listdir, sep
from typing import List, Optional, Dict, Any, Tuple
from os.path import isdir, join, normpath

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.controladores.armazenadorcaso import ArmazenadorCaso
from encadeador.modelos.caso import Caso, CasoNEWAVE, CasoDECOMP
from encadeador.modelos.dadosestudo import DadosEstudo
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.utils.log import Log


class Estudo:
    def __init__(
        self,
        dados: DadosEstudo,
        casos: List[Caso],
        estado: EstadoEstudo = EstadoEstudo.NAO_INICIADO,
    ):
        self._dados = dados
        self._casos = casos
        self._estado = estado

    @staticmethod
    def from_json(json_dict: Dict[str, Any]):
        dados = DadosEstudo.from_json(json_dict["_dados"])
        estado = EstadoEstudo.factory(json_dict["_estado"])
        return Estudo(dados, [], estado)

    def to_json(self) -> Dict[str, Any]:
        return {
            "_dados": self._dados.to_json(),
            "_estado": str(self._estado.value),
        }

    @staticmethod
    def le_arquivo_lista_casos() -> Tuple[List[str], List[str]]:
        def __cria_caminhos_casos(dirs_revisoes: List[str]) -> List[str]:
            dirs_casos: List[str] = []
            for d in dirs_revisoes:
                subd = [a for a in listdir(d) if isdir(join(d, a))]
                if Configuracoes().nome_diretorio_newave in subd:
                    c = join(
                        Configuracoes().caminho_base_estudo,
                        d,
                        Configuracoes()._nome_diretorio_newave,
                    )
                    dirs_casos.append(c)
                if Configuracoes()._nome_diretorio_decomp in subd:
                    c = join(
                        Configuracoes().caminho_base_estudo,
                        d,
                        Configuracoes()._nome_diretorio_decomp,
                    )
                    dirs_casos.append(c)
            return dirs_casos

        lista = Configuracoes().arquivo_lista_casos
        with open(lista, "r") as arq:
            dirs = arq.readlines()
        dirs = [c.strip("\n").strip() for c in dirs]
        return dirs, __cria_caminhos_casos(dirs)

    @staticmethod
    def constroi_casos(dirs: List[str]) -> List[Caso]:
        def __le_caso(c: str) -> Caso:
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
                dados = CasoNEWAVE.gera_dados_caso(c, ano, mes, rv)
                caso_nw = CasoNEWAVE(dados, [])
                return caso_nw
            elif Configuracoes()._nome_diretorio_decomp == diretorio_prog:
                dados = CasoDECOMP.gera_dados_caso(c, ano, mes, rv)
                caso_dcp = CasoDECOMP(dados, [])
                return caso_dcp
            else:
                Log.log().error(f"Diretório inválido: {diretorio_prog}")
                raise RuntimeError()

        casos: List[Caso] = []
        for c in dirs:
            try:
                caso = ArmazenadorCaso.recupera_caso(c)
                # TODO - Pensar em como permitir mudanças de diretório
                # do estudo encadeado, uma vez já concluído.
                caso.caminho = c
                casos.append(caso)
            except FileNotFoundError:
                ret = __le_caso(c)
                casos.append(ret)
        return casos

    def atualiza(self, estado: EstadoEstudo, resume: bool = False):
        Log.log().info(f"Estudo: {self._estado} - estado -> {estado.value}")
        self._estado = estado
        self._dados.resume_casos(self._casos)
        if resume:
            self._dados.resume_dados_casos(self._casos)

    @property
    def nome(self) -> str:
        return self._dados._nome

    @property
    def caminho(self) -> str:
        return self._dados._caminho

    @property
    def casos(self) -> List[Caso]:
        return self._casos

    @casos.setter
    def casos(self, c: List[Caso]):
        self._casos = c

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
                if (
                    isinstance(c, CasoNEWAVE)
                    and c.estado != EstadoCaso.CONCLUIDO
                ):
                    return c
            except ValueError:
                if isinstance(c, CasoNEWAVE):
                    return c
        return None

    @property
    def proximo_decomp(self) -> Optional[CasoDECOMP]:
        for c in self.casos:
            try:
                if (
                    isinstance(c, CasoDECOMP)
                    and c.estado != EstadoCaso.CONCLUIDO
                ):
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
                if (
                    isinstance(c, CasoNEWAVE)
                    and c.estado == EstadoCaso.CONCLUIDO
                ):
                    c_convergido = c
            except ValueError:
                break
        return c_convergido

    @property
    def ultimo_decomp(self) -> Optional[CasoDECOMP]:
        c_convergido = None
        for c in self.casos:
            try:
                if (
                    isinstance(c, CasoDECOMP)
                    and c.estado == EstadoCaso.CONCLUIDO
                ):
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

    @property
    def dados(self) -> DadosEstudo:
        return self._dados

    @property
    def estado(self) -> EstadoEstudo:
        return self._estado
