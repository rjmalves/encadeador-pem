from datetime import datetime
from typing import List

from encadeador.modelos.programa import Programa
from encadeador.modelos.runstatus import RunStatus
from encadeador.modelos.rodada import Rodada
from encadeador.modelos.estadocaso import EstadoCaso


class Caso:
    """
    Classe base que representa todos os casos executados no
    estudo encadeado. É responsável por armazenar informações
    associadas ao caso. Não lida com a sua execução, pré ou pós
    processamento dos decks.
    """

    def __init__(
        self,
        caminho: str,
        nome: str,
        ano: int,
        mes: int,
        revisao: int,
        programa: Programa,
        estado: EstadoCaso,
        id_estudo: int,
    ) -> None:
        self.id = None
        self.caminho = str(caminho)
        self.nome = nome
        self.ano = ano
        self.mes = mes
        self.revisao = revisao
        self.programa = programa
        self.estado = estado
        self.id_estudo = id_estudo
        self.rodadas: List[Rodada] = []

    def __eq__(self, o: object):
        if not isinstance(o, Caso):
            return False
        return all(
            [
                self.id == o.id,
                self.caminho == o.caminho,
                self.nome == o.nome,
                self.ano == o.ano,
                self.mes == o.mes,
                self.revisao == o.revisao,
                self.programa == o.programa,
                self.estado == o.estado,
                self.id_estudo == o.id_estudo,
            ]
        )

    def __ge__(self, o: object):
        if not isinstance(o, Caso):
            return False
        data_caso = datetime(self.ano, self.mes, 1)
        data_o = datetime(o.ano, o.mes, 1)
        if data_caso == data_o:
            if self.revisao == o.revisao:
                if (self.programa == Programa.DECOMP) and (
                    o.programa == Programa.NEWAVE
                ):
                    return True
                elif self.programa == o.programa:
                    return True
                else:
                    return False
            else:
                return self.revisao >= o.revisao
        else:
            return data_caso >= data_o

    def __gt__(self, o: object):
        if not isinstance(o, Caso):
            return False
        data_caso = datetime(self.ano, self.mes, 1)
        data_o = datetime(o.ano, o.mes, 1)
        if data_caso == data_o:
            if self.revisao == o.revisao:
                if (self.programa == Programa.DECOMP) and (
                    o.programa == Programa.NEWAVE
                ):
                    return True
                elif self.programa == o.programa:
                    return False
                else:
                    return False
            else:
                return self.revisao > o.revisao
        else:
            return data_caso > data_o

    def __le__(self, o: object):
        return not self.__gt__(o)

    def __lt__(self, o: object):
        return not self.__ge__(o)

    @property
    def tempo_execucao(self) -> float:
        return sum([j.tempo_execucao for j in self.rodadas])

    @property
    def numero_flexibilizacoes(self) -> int:
        return len(self.rodadas) - 1
