from os.path import isfile
from os.path import join
import time
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.dadosestudo import DadosEstudo

from encadeador.modelos.estudo import Estudo
from encadeador.utils.log import Log
from encadeador.utils.io import le_arquivo_json, escreve_arquivo_json

NOME_ARQUIVO_ESTADO = "estudo_encadeado.json"


class ArmazenadorEstudo:

    def __init__(self,
                 estudo: Estudo) -> None:
        self._estudo = estudo

    def armazena_estudo(self) -> bool:
        try:
            caminho = join(self._estudo.caminho, NOME_ARQUIVO_ESTADO)
            dados = self._estudo.to_json()
            escreve_arquivo_json(caminho, dados)
            return True
        except Exception as e:
            Log.log().error("Erro no armazenamento do estudo" +
                            f" {self._estudo.nome}: {e}")
            return False

    @staticmethod
    def gera_estudo() -> Estudo:

        # Se não tem arquivo de resumo, o estudo não começou a ser rodado
        nome = Configuracoes().nome_estudo
        caminho = Configuracoes().caminho_base_estudo
        arq = join(caminho, NOME_ARQUIVO_ESTADO)
        if not isfile(arq):
            # Cria um novo estudo
            dirs_revisoes, dirs_casos = Estudo.le_arquivo_lista_casos()
            casos = Estudo.constroi_casos(dirs_casos)
            dados = DadosEstudo(nome,
                                caminho,
                                time.time(),
                                dirs_revisoes,
                                dirs_casos,
                                [c.nome for c in casos],
                                [c.tempo_fila for c in casos],
                                [c.tempo_execucao for c in casos],
                                [c.estado for c in casos])
            return Estudo(dados,
                          casos)

        # Se tem, então o estudo pelo menos começou. Então constroi
        # o estudo a partir dos detalhes nos arquivos de cada caso
        e = Estudo.from_json(le_arquivo_json(arq))
        e.casos = Estudo.constroi_casos(e._dados._diretorios_casos)
        return e
