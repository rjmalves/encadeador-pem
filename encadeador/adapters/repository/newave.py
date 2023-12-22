from abc import ABC, abstractmethod
from typing import Dict, Type
from os.path import join
import pathlib
from inewave.newave.caso import Caso as ArquivoCaso
from inewave.newave.arquivos import Arquivos
from inewave.newave.dger import Dger
from inewave.newave.hidr import Hidr
from inewave.newave.cvar import Cvar
from inewave.newave.confhd import Confhd
from inewave.newave.modif import Modif
from inewave.newave.eafpast import Eafpast
from inewave.newave.adterm import Adterm
from inewave.newave.term import Term
from inewave.newave.re import Re
from inewave.newave.pmo import Pmo
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.utils.encoding import converte_codificacao


class AbstractNewaveRepository(ABC):
    @property
    @abstractmethod
    def arquivos(self) -> Arquivos:
        raise NotImplementedError

    @abstractmethod
    async def get_dger(self) -> Dger:
        raise NotImplementedError

    @abstractmethod
    def set_dger(self, d: Dger):
        raise NotImplementedError

    @abstractmethod
    def get_hidr(self) -> Hidr:
        raise NotImplementedError

    @abstractmethod
    def get_cvar(self) -> Cvar:
        raise NotImplementedError

    @abstractmethod
    def set_cvar(self, d: Cvar):
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
    def get_eafpast(self) -> Eafpast:
        raise NotImplementedError

    @abstractmethod
    def set_eafpast(self, d: Eafpast):
        raise NotImplementedError

    @abstractmethod
    def get_adterm(self) -> Adterm:
        raise NotImplementedError

    @abstractmethod
    def set_adterm(self, d: Adterm):
        raise NotImplementedError

    @abstractmethod
    def get_term(self) -> Term:
        raise NotImplementedError

    @abstractmethod
    def set_term(self, d: Term):
        raise NotImplementedError

    @abstractmethod
    def get_re(self) -> Re:
        raise NotImplementedError

    @abstractmethod
    def set_re(self, d: Re):
        raise NotImplementedError

    @abstractmethod
    def get_pmo(self) -> Pmo:
        raise NotImplementedError


class FSNewaveRepository(AbstractNewaveRepository):
    def __init__(self, path: str):
        self.__path = path
        self.__caso = ArquivoCaso.read(join(self.__path, "caso.dat"))
        self.__arquivos = Arquivos.read(
            join(self.__path, self.__caso.arquivos)
        )

    @property
    def caminho(self) -> str:
        return self.__path

    @property
    def arquivos(self) -> Arquivos:
        return self.__arquivos

    async def get_dger(self) -> Dger:
        arq_dger = self.arquivos.dger
        if arq_dger is None:
            raise FileNotFoundError("Nome do arquivo dger não especificado")
        caminho = pathlib.Path(self.__path).joinpath(arq_dger)
        await converte_codificacao(
            str(caminho), Configuracoes().script_converte_codificacao
        )
        return Dger.read(str(caminho))

    def set_dger(self, d: Dger):
        arq = self.arquivos.dger
        if arq is None:
            raise FileNotFoundError("Nome do arquivo dger não especificado")
        d.write(join(self.__path, arq))

    def get_hidr(self) -> Hidr:
        return Hidr.read(join(self.__path, "hidr.dat"))

    def get_cvar(self) -> Cvar:
        arq = self.arquivos.cvar
        if arq is None:
            raise FileNotFoundError("Nome do arquivo cvar não especificado")
        return Cvar.read(join(self.__path, arq))

    def set_cvar(self, d: Cvar):
        arq = self.arquivos.cvar
        if arq is None:
            raise FileNotFoundError("Nome do arquivo cvar não especificado")
        d.write(join(self.__path, arq))

    def get_confhd(self) -> Confhd:
        arq = self.arquivos.confhd
        if arq is None:
            raise FileNotFoundError("Nome do arquivo confhd não especificado")
        return Confhd.read(join(self.__path, arq))

    def set_confhd(self, d: Confhd):
        arq = self.arquivos.confhd
        if arq is None:
            raise FileNotFoundError("Nome do arquivo confhd não especificado")
        d.write(join(self.__path, arq))

    def get_modif(self) -> Modif:
        arq = self.arquivos.modif
        if arq is None:
            raise FileNotFoundError("Nome do arquivo modif não especificado")
        return Modif.read(join(self.__path, arq))

    def set_modif(self, d: Modif):
        arq = self.arquivos.modif
        if arq is None:
            raise FileNotFoundError("Nome do arquivo modif não especificado")
        d.write(join(self.__path, arq))

    def get_eafpast(self) -> Eafpast:
        arq = self.arquivos.vazpast
        if arq is None:
            raise FileNotFoundError("Nome do arquivo eafpast não especificado")

        return Eafpast.read(join(self.__path, arq))

    def set_eafpast(self, d: Eafpast):
        arq = self.arquivos.vazpast
        if arq is None:
            raise FileNotFoundError("Nome do arquivo eafpast não especificado")

        d.write(join(self.__path, arq))

    def get_adterm(self) -> Adterm:
        arq = self.arquivos.adterm
        if arq is None:
            raise FileNotFoundError("Nome do arquivo adterm não especificado")

        return Adterm.read(join(self.__path, arq))

    def set_adterm(self, d: Adterm):
        arq = self.arquivos.adterm
        if arq is None:
            raise FileNotFoundError("Nome do arquivo adterm não especificado")

        d.write(join(self.__path, arq))

    def get_term(self) -> Term:
        arq = self.arquivos.term
        if arq is None:
            raise FileNotFoundError("Nome do arquivo term não especificado")

        return Term.read(join(self.__path, arq))

    def set_term(self, d: Term):
        arq = self.arquivos.term
        if arq is None:
            raise FileNotFoundError("Nome do arquivo term não especificado")

        d.write(join(self.__path, arq))

    def get_re(self) -> Re:
        arq = self.arquivos.re
        if arq is None:
            raise FileNotFoundError("Nome do arquivo re não especificado")

        return Re.read(join(self.__path, arq))

    def set_re(self, d: Re):
        arq = self.arquivos.re
        if arq is None:
            raise FileNotFoundError("Nome do arquivo re não especificado")

        d.write(join(self.__path, arq))

    def get_pmo(self) -> Pmo:
        arq = self.arquivos.pmo
        if arq is None:
            raise FileNotFoundError("Nome do arquivo pmo não especificado")

        return Pmo.read(join(self.__path, arq))


def factory(kind: str, *args, **kwargs) -> AbstractNewaveRepository:
    mappings: Dict[str, Type[AbstractNewaveRepository]] = {
        "FS": FSNewaveRepository,
    }
    return mappings[kind](*args, **kwargs)
