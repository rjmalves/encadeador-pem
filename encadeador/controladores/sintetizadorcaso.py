from abc import abstractmethod
from genericpath import isfile
from os import listdir, remove
from os.path import join, isdir
import pandas as pd
from shutil import move

from encadeador.modelos.caso import Caso
from encadeador.modelos.programa import Programa
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.adapters.repository.synthesis import (
    factory as synthesis_factory,
)
from encadeador.services.unitofwork.newave import factory as nw_uow_factory
from encadeador.services.unitofwork.decomp import factory as dc_uow_factory
from encadeador.utils.log import Log
from encadeador.utils.terminal import executa_terminal_retry


class SintetizadorCaso:
    def __init__(self, caso: Caso) -> None:
        self._caso = caso

    @staticmethod
    def factory(caso: Caso) -> "SintetizadorCaso":
        if caso.programa == Programa.NEWAVE:
            return SintetizadorNEWAVE(caso)
        elif caso.programa == Programa.DECOMP:
            return SintetizadorDECOMP(caso)
        else:
            raise ValueError(f"Caso não suportado")

    @abstractmethod
    def sintetiza_caso(self, comando: str) -> bool:
        pass

    @property
    def caso(self) -> Caso:
        return self._caso


class SintetizadorNEWAVE(SintetizadorCaso):
    def __init__(self, caso: Caso):
        super().__init__(caso)

    def sintetiza_caso(self, comando: str) -> bool:
        Log.log().info(
            "Sintetizando informações do" + f" caso {self._caso.nome}"
        )
        uow = nw_uow_factory("FS", self._caso.caminho)
        with uow:
            comando = f"sintetizador-newave {comando} --formato {Configuracoes().formato_sintese}"
            cod, res = executa_terminal_retry(comando, timeout=600)
        if cod != 0:
            Log.log().error(f"Erro na síntese do NEWAVE: {res}")
        return cod == 0


class SintetizadorDECOMP(SintetizadorCaso):
    def __init__(self, caso: Caso):
        super().__init__(caso)
        self.__repositorio_sintese = synthesis_factory(
            Configuracoes().formato_sintese
        )

    def __backup_inviabilidades(self):
        if isdir(Configuracoes().diretorio_sintese):
            arquivos_inviabilidades = [
                a
                for a in listdir(Configuracoes().diretorio_sintese)
                if "INVIABILIDADE" in a and "bkp-" not in a
            ]
            for a in arquivos_inviabilidades:
                caminho = join(Configuracoes().diretorio_sintese, a)
                caminho_bkp = join(
                    Configuracoes().diretorio_sintese, "bkp-" + a
                )
                move(caminho, caminho_bkp)

    def __acumula_inviabilidades(self):
        arquivos_inviabilidades = [
            a
            for a in listdir(Configuracoes().diretorio_sintese)
            if "INVIABILIDADE" in a and "bkp-" not in a
        ]
        arquivos_sem_extensao = [
            a.split(".")[0] for a in arquivos_inviabilidades
        ]
        for a in arquivos_sem_extensao:
            caminho = join(Configuracoes().diretorio_sintese, a)
            df = self.__repositorio_sintese.read(caminho)
            caminho_bkp = join(Configuracoes().diretorio_sintese, "bkp-" + a)
            df_bkp = (
                pd.DataFrame()
                if not isfile(caminho_bkp)
                else self.__repositorio_sintese.read(caminho_bkp)
            )
            cols_df = df.columns.tolist()
            df["Flexibilizacao"] = self._caso.numero_flexibilizacoes
            df = df[["Flexibilizacao"] + cols_df]
            df = pd.concat([df_bkp, df], ignore_index=True)
            self.__repositorio_sintese.write(df, caminho)

    def __limpa_backup_inviabilidades(self):
        arquivos_backup = [
            a
            for a in listdir(Configuracoes().diretorio_sintese)
            if "INVIABILIDADE" in a and "bkp-" in a
        ]
        for a in arquivos_backup:
            caminho = join(Configuracoes().diretorio_sintese, a)
            remove(caminho)

    def sintetiza_caso(self, comando: str) -> bool:
        Log.log().info(
            "Sintetizando informações do" + f" caso {self._caso.nome}"
        )
        uow = dc_uow_factory("FS", self._caso.caminho)
        with uow:
            self.__backup_inviabilidades()
            comando = f"sintetizador-decomp {comando} --formato {Configuracoes().formato_sintese}"
            cod, res = executa_terminal_retry(comando, timeout=600)
            if cod != 0:
                Log.log().error(f"Erro na síntese do DECOMP: {res}")
            self.__acumula_inviabilidades()
            self.__limpa_backup_inviabilidades()
        return cod == 0
