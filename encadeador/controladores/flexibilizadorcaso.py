from abc import abstractmethod
from typing import List

from encadeador.services.unitofwork.decomp import factory as dc_uow_factory
from encadeador.modelos.caso import Caso
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.inviabilidade import Inviabilidade
from encadeador.modelos.programa import Programa
from encadeador.modelos.regraflexibilizacao import RegraFlexibilizacao
from encadeador.utils.log import Log


class Flexibilizador:
    def __init__(self, caso: Caso):
        self._caso = caso

    @staticmethod
    def factory(caso: Caso) -> "Flexibilizador":
        if caso.programa == Programa.NEWAVE:
            return FlexibilizadorNEWAVE(caso)
        if caso.programa == Programa.DECOMP:
            return FlexibilizadorDECOMP(caso)

    @abstractmethod
    def flexibiliza(self) -> bool:
        raise NotImplementedError


class FlexibilizadorNEWAVE(Flexibilizador):
    def __init__(self, caso: Caso):
        super().__init__(caso)

    def flexibiliza(self) -> bool:
        raise NotImplementedError


class FlexibilizadorDECOMP(Flexibilizador):
    def __init__(self, caso: Caso):
        super().__init__(caso)
        self._uow = dc_uow_factory("FS", caso.caminho)

    def flexibiliza(self) -> bool:
        max_flex = Configuracoes().maximo_flexibilizacoes_revisao
        Log.log().info(
            f"Flexibilizando caso {self._caso.nome}: "
            + f"{self._caso.numero_flexibilizacoes } de {max_flex}"
        )
        with self._uow:
            try:
                inviab = self._uow.decomp.get_inviab()
                dadger = self._uow.decomp.get_dadger()
                hidr = self._uow.decomp.get_hidr()
                relato = self._uow.decomp.get_relato()
                # Cria as inviabilidades
                inviabilidades: List[Inviabilidade] = []
                for (
                    _,
                    linha,
                ) in inviab.inviabilidades_simulacao_final.iterrows():
                    inv = Inviabilidade.factory(linha, hidr, relato)
                    Log.log().info(inv)
                    inviabilidades.append(inv)
                Log.log().info("Inviabilidades processadas com sucesso")
                # Cria a regra de flexibilização
                metodo_flex = Configuracoes().metodo_flexibilizacao
                regra = RegraFlexibilizacao.factory(metodo_flex)
                # Flexibiliza
                regra.flexibiliza(dadger, inviabilidades)
                Log.log().info("Inviabilidades flexibilizadas")
                # Escreve o dadger.rvX de saída
                self._uow.decomp.set_dadger(dadger)
            except Exception as e:
                Log.log().exception(e)
                return False
        return True
