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
