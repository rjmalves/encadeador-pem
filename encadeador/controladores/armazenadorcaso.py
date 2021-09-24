from genericpath import isfile
import pandas as pd
from logging import Logger
from os.path import join

from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.caso import Caso, CasoDECOMP, CasoNEWAVE
from encadeador.modelos.caso import Configuracoes

NOME_ARQUIVO_ESTADO = "caso_encadeado.csv"


class ArmazenadorCaso:

    def __init__(self,
                 caso: Caso,
                 log: Logger) -> None:
        self._caso = caso
        self._log = log

    def armazena_caso(self, estado: EstadoJob) -> bool:
        try:
            dados = {
                     "Caminho": [self.caso.caminho],
                     "Nome": [self.caso.nome],
                     "Ano": [self.caso.ano],
                     "Mes": [self.caso.mes],
                     "Revisao": [self.caso.revisao],
                     "Estado": [str(estado.value)],
                     "Tentativas": [self.caso.numero_tentativas],
                     "Processadores": [self.caso.numero_processadores],
                     "Sucesso": [self.caso.sucesso],
                     "Entrada Fila": [self.caso.instante_entrada_fila],
                     "Inicio Execucao": [self.caso.instante_inicio_execucao],
                     "Fim Execucao": [self.caso.instante_fim_execucao],
                    }
            if isinstance(self.caso, CasoNEWAVE):
                dados["Programa"] = "NEWAVE"
            elif isinstance(self.caso, CasoDECOMP):
                dados["Programa"] = "DECOMP"
            else:
                raise ValueError("Tipo de casos não" +
                                 f" suportado: {type(self.caso)}")
            df = pd.DataFrame(data=dados)
            df.to_csv(join(self.caso.caminho, NOME_ARQUIVO_ESTADO),
                    header=True,
                    encoding="utf-8")
            return True
        except Exception as e:
            self._log.error("Erro no armazenamento do caso" +
                            f" {self._caso.nome}: {e}")
            return False
    
    @staticmethod
    def recupera_caso(cfg: Configuracoes,
                      caminho: str) -> Caso:
        # Se não tem arquivo de resumo, o caso não começou a ser rodado
        arq = join(caminho, NOME_ARQUIVO_ESTADO)
        if not isfile(arq):
            raise FileNotFoundError("Não encontrado arquivo de resumo" +
                                    f"de caso no diretório {caminho}.")
        # Se tem, então o caso pelo menos começou
        df = pd.read_csv(arq)
        prog = str(df.loc[:, "Programa"])
        if prog == "NEWAVE":
            c = CasoNEWAVE()
        elif prog == "DECOMP":
            c = CasoDECOMP()
        else:
            raise ValueError(f"Programa {prog} não suportado")
        # Atribui os dados armazenados
        c._caminho_caso = str(df.loc[:, "Caminho"])
        c._nome_caso = str(df.loc[:, "Nome"])
        c._ano_caso = int(df.loc[:, "Ano"])
        c._mes_caso = int(df.loc[:, "Mes"])
        c._revisao_caso = int(df.loc[:, "Revisao"])
        c._configuracoes = cfg
        c._instante_entrada_fila = str(df.loc[:, "Entrada Fila"])
        c._instante_inicio_execucao = str(df.loc[:, "Inicio Execucao"])
        c._instante_fim_execucao = str(df.loc[:, "Fim Execucao"])
        c._numero_tentativas = str(df.loc[:, "Tentativas"])
        c._numero_processadores = str(df.loc[:, "Processadores"])
        c._sucesso = str(df.loc[:, "Sucesso"])
        return c

    @property
    def caso(self) -> Caso:
        return self._caso
