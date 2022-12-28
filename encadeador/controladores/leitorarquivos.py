from typing import List
from pathlib import Path
from os import listdir

from encadeador.utils.log import Log
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.regrainviabilidade import RegraInviabilidade
from encadeador.modelos.regrareservatorio import RegraReservatorio


class LeitorArquivos:
    @staticmethod
    def carrega_lista_casos() -> List[str]:
        def __processa_subdiretorios(diretorios: str) -> List[str]:
            casos: List[str] = []
            for d in diretorios:
                caminho = Path(Configuracoes().caminho_base_estudo).joinpath(d)
                subdiretorios = [
                    a for a in listdir(caminho) if caminho.joinpath(a).is_dir()
                ]
                for p in [
                    Configuracoes().nome_diretorio_newave,
                    Configuracoes().nome_diretorio_decomp,
                ]:
                    if p in subdiretorios:
                        casos.append(str(caminho.joinpath(p)))
            return casos

        lista_casos: List[str] = []
        try:
            with open(Configuracoes().arquivo_lista_casos, "r") as arq:
                diretorios_casos = arq.readlines()
            diretorios_casos = [
                c.strip("\n").strip() for c in diretorios_casos
            ]
            lista_casos = __processa_subdiretorios(diretorios_casos)
        except FileNotFoundError:
            Log.log().warning("Arquivo com a lista dos casos não encontrado")
        return lista_casos

    @staticmethod
    def carrega_regras_reservatorios() -> List[RegraReservatorio]:
        arq_regras_reserv = (
            Configuracoes().arquivo_regras_operacao_reservatorios
        )
        regras_reserv: List[RegraReservatorio] = []
        try:
            if arq_regras_reserv is not None:
                regras_reserv = RegraReservatorio.from_csv(arq_regras_reserv)
        except FileNotFoundError:
            Log.log().warning(
                "Arquivo de regras de reservatórios não encontrado"
            )
        return regras_reserv

    @staticmethod
    def carrega_regras_inviabilidades() -> List[RegraInviabilidade]:
        Log.log().warning("Arquivo de regras de inviabilidades não suportado")
        return []
