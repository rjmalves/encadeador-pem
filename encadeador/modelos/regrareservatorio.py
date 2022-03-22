from idecomp.decomp.dadger import Dadger

import pandas as pd  # type: ignore
from typing import List, Dict, Any


class RegraReservatorio:
    def __init__(
        self,
        _codigo_reservatorio: int,
        _codigo_usina: int,
        _tipo_restricao: str,
        _mes: int,
        _volume_minimo: float,
        _volume_maximo: float,
        _limite_minimo: float,
        _limite_maximo: float,
        _periodicidade: str,
        _legenda_faixa: str,
    ):
        self._codigo_reservatorio = _codigo_reservatorio
        self._codigo_usina = _codigo_usina
        self._tipo_restricao = _tipo_restricao
        self._mes = _mes
        self._volume_minimo = _volume_minimo
        self._volume_maximo = _volume_maximo
        self._limite_minimo = _limite_minimo
        self._limite_maximo = _limite_maximo
        self._periodicidade = _periodicidade
        self._legenda_faixa = _legenda_faixa

    def __str__(self) -> str:
        return (
            f"Regra: reservatório {self._codigo_reservatorio}"
            + f" -> usina {self._codigo_usina} | {self._tipo_restricao}"
            + f" mês {self._mes}. Faixa {self._legenda_faixa}: "
            + f" ({self._volume_minimo}, {self._volume_maximo})"
            + f" -> ({self._limite_minimo},{self._limite_maximo}). "
            + f" Periodicidade {self._periodicidade}"
        )

    @staticmethod
    def from_csv(caminho: str) -> List["RegraReservatorio"]:
        df = pd.read_csv(caminho, index_col=None, sep=";")
        df = df.loc[~df["COD_RESERVATORIO_VOL"].str.contains("&")]
        regras: List[RegraReservatorio] = []
        for _, linha in df.iterrows():
            regras.append(
                RegraReservatorio(
                    int(linha["COD_RESERVATORIO_VOL"]),
                    int(linha["CODIGO_USINA_RESTRICAO"]),
                    linha["TIPO_REST"],
                    int(linha["MES"]),
                    float(linha["VOL_MIN"]),
                    float(linha["VOL_MAX"]),
                    float(linha["LIM_MIN"]),
                    float(linha["LIM_MAX"]),
                    linha["PERIOD"],
                    linha["LEGENDA_FAIXA"],
                )
            )
        return regras

    @staticmethod
    def from_json(json_dict: Dict[str, Any]) -> "RegraReservatorio":
        return RegraReservatorio(**json_dict)

    def to_json(self) -> Dict[str, Any]:
        return self.__dict__

    @property
    def codigo_reservatorio(self) -> int:
        return self._codigo_reservatorio

    @property
    def codigo_usina(self) -> int:
        return self._codigo_usina

    @property
    def tipo_restricao(self) -> str:
        return self._tipo_restricao

    @property
    def mes(self) -> int:
        return self._mes

    @property
    def volume_minimo(self) -> float:
        return self._volume_minimo

    @property
    def volume_maximo(self) -> float:
        return self._volume_maximo

    @property
    def limite_minimo(self) -> float:
        return self._limite_minimo

    @property
    def limite_maximo(self) -> float:
        return self._limite_maximo

    @property
    def periodicidade(self) -> str:
        return self._periodicidade
