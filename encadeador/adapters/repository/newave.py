from abc import ABC, abstractmethod
from typing import Dict
from encadeador.modelos.configuracoes import Configuracoes
from os.path import join
from inewave.newave.caso import Caso as ArquivoCaso
from inewave.newave.arquivos import Arquivos
from inewave.newave.dger import DGer
from inewave.newave.hidr import Hidr
from inewave.newave.cvar import CVAR
from inewave.newave.confhd import Confhd
from inewave.newave.modif import Modif
from inewave.newave.eafpast import EafPast
from inewave.newave.adterm import AdTerm
from inewave.newave.term import Term
from inewave.newave.re import RE
from inewave.newave.pmo import PMO


class AbstractNewaveRepository(ABC):
    @property
    @abstractmethod
    def arquivos(self) -> Arquivos:
        raise NotImplementedError

    @abstractmethod
    def get_dger(self) -> DGer:
        raise NotImplementedError

    @abstractmethod
    def set_dger(self, d: DGer):
        raise NotImplementedError

    @abstractmethod
    def get_hidr(self) -> Hidr:
        raise NotImplementedError

    @abstractmethod
    def get_cvar(self) -> CVAR:
        raise NotImplementedError

    @abstractmethod
    def set_cvar(self, d: CVAR):
        raise NotImplementedError

    @abstractmethod
    def get_confhd(self) -> Confhd:
        raise NotImplementedError

    @abstractmethod
    def set_confhd(self, d: Confhd):
        raise NotImplementedError

    @abstractmethod
    def get_modif(self) -> Modif:
        raise NotImplementedError

    @abstractmethod
    def set_modif(self, d: Modif):
        raise NotImplementedError

    @abstractmethod
    def get_eafpast(self) -> EafPast:
        raise NotImplementedError

    @abstractmethod
    def set_eafpast(self, d: EafPast):
        raise NotImplementedError

    @abstractmethod
    def get_adterm(self) -> AdTerm:
        raise NotImplementedError

    @abstractmethod
    def set_adterm(self, d: AdTerm):
        raise NotImplementedError

    @abstractmethod
    def get_term(self) -> Term:
        raise NotImplementedError

    @abstractmethod
    def set_term(self, d: Term):
        raise NotImplementedError

    @abstractmethod
    def get_re(self) -> RE:
        raise NotImplementedError

    @abstractmethod
    def set_re(self, d: RE):
        raise NotImplementedError

    @abstractmethod
    def get_pmo(self) -> PMO:
        raise NotImplementedError


class FSNewaveRepository(AbstractNewaveRepository):
    def __init__(self, path: str):
        self.__path = join(Configuracoes().caminho_base_estudo, path)
        self.__caso = ArquivoCaso.le_arquivo(self.__path)
        self.__arquivos = Arquivos.le_arquivo(
            self.__path, self.__caso.arquivos
        )

    @property
    def caminho(self) -> str:
        return self.__path

    @property
    def arquivos(self) -> Arquivos:
        return self.__arquivos

    def get_dger(self) -> DGer:
        return DGer.le_arquivo(self.__path, self.arquivos.dger)

    def set_dger(self, d: DGer):
        d.escreve_arquivo(self.__path, self.__arquivos.dger)

    @abstractmethod
    def get_hidr(self) -> Hidr:
        return Hidr.le_arquivo(self.__path, "hidr.dat")

    def get_cvar(self) -> CVAR:
        return CVAR.le_arquivo(self.__path, self.arquivos.cvar)

    def set_cvar(self, d: CVAR):
        d.escreve_arquivo(self.__path, self.__arquivos.cvar)

    def get_confhd(self) -> Confhd:
        return Confhd.le_arquivo(self.__path, self.arquivos.confhd)

    def set_confhd(self, d: CVAR):
        d.escreve_arquivo(self.__path, self.__arquivos.confhd)

    def get_modif(self) -> Modif:
        return Modif.le_arquivo(self.__path, self.arquivos.modif)

    def set_modif(self, d: CVAR):
        d.escreve_arquivo(self.__path, self.__arquivos.modif)

    def get_eafpast(self) -> EafPast:
        return EafPast.le_arquivo(self.__path, self.arquivos.vazpast)

    def set_eafpast(self, d: EafPast):
        d.escreve_arquivo(self.__path, self.__arquivos.vazpast)

    def get_adterm(self) -> AdTerm:
        return AdTerm.le_arquivo(self.__path, self.arquivos.adterm)

    def set_adterm(self, d: AdTerm):
        d.escreve_arquivo(self.__path, self.__arquivos.adterm)

    def get_term(self) -> Term:
        return Term.le_arquivo(self.__path, self.arquivos.term)

    def set_term(self, d: Term):
        d.escreve_arquivo(self.__path, self.__arquivos.term)

    def get_re(self) -> RE:
        return RE.le_arquivo(self.__path, self.arquivos.re)

    def set_re(self, d: RE):
        d.escreve_arquivo(self.__path, self.__arquivos.re)

    def get_pmo(self) -> PMO:
        return PMO.le_arquivo(self.__path, self.arquivos.pmo)


# TODO
class S3NewaveRepository(ABC):
    def __init__(self, url: str):
        self.__url = url

    @property
    def arquivos(self) -> Arquivos:
        raise NotImplementedError


def factory(kind: str, *args, **kwargs) -> AbstractNewaveRepository:
    mappings: Dict[str, AbstractNewaveRepository] = {
        "FS": FSNewaveRepository,
        "S3": S3NewaveRepository,
    }
    return mappings[kind](*args, **kwargs)
