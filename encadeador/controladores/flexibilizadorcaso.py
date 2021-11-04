from abc import abstractmethod
from logging import Logger
from typing import List
from idecomp.decomp.inviabunic import InviabUnic
from idecomp.decomp.dadger import Dadger
from idecomp.decomp.hidr import Hidr

from encadeador.modelos.caso import Caso, CasoDECOMP
from encadeador.modelos.inviabilidade import Inviabilidade
from encadeador.modelos.regraflexibilizacao import RegraFlexibilizacao


class Flexibilizador:

    def __init__(self,
                 caso: Caso,
                 log: Logger):
        self._caso = caso
        self._log = log

    @staticmethod
    def factory(caso: Caso,
                log: Logger) -> 'Flexibilizador':
        if isinstance(caso, CasoDECOMP):
            return FlexibilizadorDECOMP(caso,
                                        log)
        else:
            raise TypeError(f"Caso do tipo {type(caso)} " +
                            "não suportado para encadeamento")

    @abstractmethod
    def flexibiliza(self) -> bool:
        raise RuntimeError("Não se deve flexibilizar NEWAVE")


class FlexibilizadorDECOMP(Flexibilizador):

    def __init__(self,
                 caso: Caso,
                 log: Logger):
        super().__init__(caso, log)

    def flexibiliza(self) -> bool:
        max_flex = self._caso.configuracoes.maximo_flexibilizacoes_revisao
        self._caso.adiciona_flexibilizacao()
        self._log.info(f"Flexibilizando caso {self._caso.nome}: " +
                       f"{self._caso.numero_flexibilizacoes } de {max_flex}")
        try:
            # Lê o inviab_unic.rvX
            arq_inviab = f"inviab_unic.rv{self._caso.revisao}"
            inviab = InviabUnic.le_arquivo(self._caso.caminho, arq_inviab)
            self._log.info(f"Arquivo {arq_inviab} lido com sucesso")
            # Lê o dadger.rvX
            arq_dadger = f"dadger.rv{self._caso.revisao}"
            dadger = Dadger.le_arquivo(self._caso.caminho, arq_dadger)
            self._log.info(f"Arquivo {arq_dadger} lido com sucesso")
            # Lê o hidr.dat
            hidr = Hidr.le_arquivo(self._caso.caminho)
            self._log.info("Arquivo hidr.dat lido com sucesso")
            # Cria as inviabilidades
            inviabilidades: List[Inviabilidade] = []
            for _, linha in inviab.inviabilidades_simulacao_final.iterrows():
                inv = Inviabilidade.factory(linha, hidr)
                self._log.info(inv)
                inviabilidades.append(inv)
            self._log.info("Inviabilidades processadas com sucesso")
            # Cria a regra de flexibilização
            metodo_flex = self._caso.configuracoes.metodo_flexibilizacao
            regra = RegraFlexibilizacao.factory(metodo_flex, self._log)
            # Flexibiliza
            regra.flexibiliza(dadger, inviabilidades)
            self._log.info("Inviabilidades flexibilizadas")
            # Escreve o dadger.rvX de saída
            dadger.escreve_arquivo(self._caso.caminho, arq_dadger)
            self._log.info(f"Arquivo {arq_dadger} escrito com sucesso")
            return True
        except Exception as e:
            self._log.error(e)
            return False
