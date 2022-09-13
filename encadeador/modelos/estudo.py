from typing import List, Optional
from encadeador.modelos.caso import Caso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.programa import Programa
from encadeador.utils.log import Log


class Estudo:
    def __init__(self, _caminho: str, _nome: str, _estado: EstadoEstudo):
        self._id = None
        self._caminho = _caminho
        self._nome = _nome
        self._estado = _estado

    def __eq__(self, o: object):
        if not isinstance(o, Estudo):
            return False
        return all(
            [
                self.id == o.id,
                self.caminho == o.caminho,
                self.nome == o.nome,
                self.estado == o.estado,
            ]
        )

    @property
    def id(self) -> Optional[int]:
        return self._id

    @property
    def nome(self) -> str:
        return self._nome

    @nome.setter
    def nome(self, n: str):
        self._nome = n

    @property
    def caminho(self) -> str:
        return self._caminho

    @caminho.setter
    def caminho(self, c: str):
        self._caminho = c

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
    def proximo_newave(self) -> Optional[Caso]:
        for c in self.casos:
            try:
                if (
                    c.programa == Programa.NEWAVE
                    and c.estado != EstadoCaso.CONCLUIDO
                ):
                    return c
            except ValueError:
                if c.programa == Programa.NEWAVE:
                    return c
        return None

    @property
    def proximo_decomp(self) -> Optional[Caso]:
        for c in self.casos:
            try:
                if (
                    c.programa == Programa.DECOMP
                    and c.estado != EstadoCaso.CONCLUIDO
                ):
                    return c
            except ValueError:
                if c.programa == Programa.DECOMP:
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
    def ultimo_newave(self) -> Optional[Caso]:
        c_convergido = None
        for c in self.casos:
            try:
                if (
                    c.programa == Programa.NEWAVE
                    and c.estado == EstadoCaso.CONCLUIDO
                ):
                    c_convergido = c
            except ValueError:
                break
        return c_convergido

    @property
    def ultimo_decomp(self) -> Optional[Caso]:
        c_convergido = None
        for c in self.casos:
            try:
                if (
                    c.programa == Programa.DECOMP
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
    def estado(self) -> EstadoEstudo:
        return self._estado

    @estado.setter
    def estado(self, e: EstadoEstudo):
        Log.log().debug(f"Estudo: {self._estado} -> {e.value}")
        self._estado = e
