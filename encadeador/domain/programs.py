from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.programa import Programa
from pathlib import Path
from typing import Tuple, Dict, Optional
from os.path import join
from os import listdir


class ProgramRules:
    @staticmethod
    def program_from_folder(dir: str) -> Optional[Programa]:
        mapping: Dict[str, Programa] = {
            Configuracoes().nome_diretorio_newave: Programa.NEWAVE,
            Configuracoes().nome_diretorio_decomp: Programa.DECOMP,
        }
        return mapping.get(dir)

    @staticmethod
    def case_from_path(path: str) -> Optional[Tuple[int, int, int, Programa]]:
        p = Path(path)
        case_data = p.parts[-2].split("_")
        if len(case_data) != 3:
            return None
        ano = int(case_data[0])
        mes = int(case_data[1])
        rv = int(case_data[2].split("rv")[1])
        programa = ProgramRules.program_from_folder(p.stem)
        if programa is None:
            return None
        return ano, mes, rv, programa

    @staticmethod
    def newave_case_name(year: int, month: int) -> str:
        return f"{Configuracoes().nome_estudo} NEWAVE {str(month).zfill(2)}/{year}"

    @staticmethod
    def decomp_case_name(year: int, month: int, rv: int) -> str:
        return f"{Configuracoes().nome_estudo} DECOMP {str(month).zfill(2)}/{year} rv{rv}"

    @staticmethod
    def case_name_from_data(
        year: int, month: int, rv: int, program: Programa
    ) -> Optional[str]:
        mapping: Dict[Programa, str] = {
            Programa.NEWAVE: ProgramRules.newave_case_name(year, month),
            Programa.DECOMP: ProgramRules.decomp_case_name(year, month, rv),
        }
        return mapping.get(program)

    @staticmethod
    def newave_job_path() -> str:
        basedir = Configuracoes().diretorio_instalacao_newaves
        version = Configuracoes().versao_newave
        versiondir = join(basedir, version)
        files = listdir(versiondir)
        jobfile = [a for a in files if ".job" in a]
        return join(versiondir, jobfile[0])

    @staticmethod
    def decomp_job_path() -> str:
        basedir = Configuracoes().diretorio_instalacao_decomps
        version = Configuracoes().versao_decomp
        versiondir = join(basedir, version)
        files = listdir(versiondir)
        jobfile = [a for a in files if ".job" in a]
        return join(versiondir, jobfile[0])

    @staticmethod
    def program_job_path(program: Programa) -> Optional[str]:
        mapping: Dict[Programa, str] = {
            Programa.NEWAVE: ProgramRules.newave_job_path(),
            Programa.DECOMP: ProgramRules.decomp_job_path(),
        }
        return mapping.get(program)

    @staticmethod
    def newave_job_name(year: int, month: int) -> str:
        return f"NW{year}{str(month).zfill(2)}"

    @staticmethod
    def decomp_job_name(year: int, month: int, rv: int) -> str:
        return f"DC{year}{str(month).zfill(2)}{rv}"

    @staticmethod
    def program_job_name(
        year: int, month: int, rv: int, program: Programa
    ) -> Optional[str]:
        mapping: Dict[Programa, str] = {
            Programa.NEWAVE: ProgramRules.newave_job_name(year, month),
            Programa.DECOMP: ProgramRules.decomp_job_name(year, month, rv),
        }
        return mapping.get(program)

    @staticmethod
    def newave_processor_count() -> int:
        return Configuracoes().processadores_minimos_newave

    @staticmethod
    def decomp_processor_count() -> int:
        return Configuracoes().processadores_minimos_decomp

    @staticmethod
    def program_processor_count(program: Programa) -> Optional[int]:
        mapping: Dict[Programa, int] = {
            Programa.NEWAVE: ProgramRules.newave_processor_count(),
            Programa.DECOMP: ProgramRules.decomp_processor_count(),
        }
        return mapping.get(program)
