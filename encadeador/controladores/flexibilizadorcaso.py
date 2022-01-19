from abc import abstractmethod
from typing import List
from idecomp.decomp.inviabunic import InviabUnic
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.hidr import Hidr
from idecomp.decomp.relato import Relato

from encadeador.modelos.caso import Caso, CasoDECOMP
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.inviabilidade import Inviabilidade
from encadeador.modelos.regraflexibilizacao import RegraFlexibilizacao
from encadeador.utils.log import Log


class Flexibilizador:

    def __init__(self,
                 caso: Caso):
        self._caso = caso

    @staticmethod
    def factory(caso: Caso) -> 'Flexibilizador':
        if isinstance(caso, CasoDECOMP):
            return FlexibilizadorDECOMP(caso)
        else:
            raise TypeError(f"Caso do tipo {type(caso)} " +
                            "não suportado para encadeamento")

    @abstractmethod
    def flexibiliza(self) -> bool:
        raise RuntimeError("Não se deve flexibilizar NEWAVE")


class FlexibilizadorDECOMP(Flexibilizador):

    def __init__(self,
                 caso: Caso):
        super().__init__(caso)

    def flexibiliza(self) -> bool:
        max_flex = Configuracoes().maximo_flexibilizacoes_revisao
        Log.log().info(f"Flexibilizando caso {self._caso.nome}: " +
                       f"{self._caso.numero_flexibilizacoes } de {max_flex}")
        try:
            # Lê o inviab_unic.rvX
            arq_inviab = f"inviab_unic.rv{self._caso.revisao}"
            inviab = InviabUnic.le_arquivo(self._caso.caminho, arq_inviab)
            Log.log().info(f"Arquivo {arq_inviab} lido com sucesso")
            # Lê o dadger.rvX
            arq_dadger = f"dadger.rv{self._caso.revisao}"
            dadger = Dadger.le_arquivo(self._caso.caminho, arq_dadger)
            Log.log().info(f"Arquivo {arq_dadger} lido com sucesso")
            # Lê o hidr.dat
            hidr = Hidr.le_arquivo(self._caso.caminho)
            Log.log().info("Arquivo hidr.dat lido com sucesso")
            # Lê o relato.rvX
            arq_relato = f"relato.rv{self._caso.revisao}"
            relato = Relato.le_arquivo(self._caso.caminho, arq_relato)
            # Cria as inviabilidades
            inviabilidades: List[Inviabilidade] = []
            for _, linha in inviab.inviabilidades_simulacao_final.iterrows():
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
            dadger.escreve_arquivo(self._caso.caminho, arq_dadger)
            Log.log().info(f"Arquivo {arq_dadger} escrito com sucesso")
            return True
        except Exception as e:
            Log.log().error(e)
            return False
