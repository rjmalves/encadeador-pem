from typing import List, Optional
from encadeador.modelos.caso import Caso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.programa import Programa


class Estudo:
    def __init__(self, caminho: str, nome: str, estado: EstadoEstudo):
        self.id = None
        self.caminho = caminho
        self.nome = nome
        self.estado = estado
        self.casos: List[Caso] = []

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

    def indice_caso(self, caso: Optional[Caso]) -> int:
        if caso is None:
            return 0
        else:
            return self.casos.index(caso)

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
