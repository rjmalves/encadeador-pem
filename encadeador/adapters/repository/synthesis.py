from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, Type
from encadeador.utils.log import Log


class AbstractSynthesisRepository(ABC):
    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def read(self, filename: str) -> pd.DataFrame:
        pass

    @abstractmethod
    def write(self, df: pd.DataFrame, filename: str) -> bool:
        pass


class ParquetSynthesisRepository(AbstractSynthesisRepository):
    def read(self, filename: str) -> pd.DataFrame:
        return pd.read_parquet(filename + ".parquet.gzip")

    def write(self, df: pd.DataFrame, filename: str) -> bool:
        df.to_parquet(filename + ".parquet.gzip", compression="gzip")
        return True


class CSVSynthesisRepository(AbstractSynthesisRepository):
    def read(self, filename: str) -> pd.DataFrame:
        return pd.read_csv(filename + ".csv")

    def write(self, df: pd.DataFrame, filename: str) -> bool:
        df.to_csv(filename + ".csv", index=False)
        return True


def factory(kind: str, *args, **kwargs) -> AbstractSynthesisRepository:
    mapping: Dict[str, Type[AbstractSynthesisRepository]] = {
        "PARQUET": ParquetSynthesisRepository,
        "CSV": CSVSynthesisRepository,
    }
    kind = kind.upper()
    if kind not in mapping.keys():
        msg = f"Formato de síntese {kind} não suportado"
        Log.log().error(msg)
        raise ValueError(msg)
    return mapping.get(kind)(*args, **kwargs)
