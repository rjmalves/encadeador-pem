import pandas as pd  # type: ignore
from typing import List, Dict, Any, Optional
from datetime import datetime


class RegraReservatorio:
    def __init__(
        self,
        _inicio_vigencia: Optional[str],
        _fim_vigencia: Optional[str],
        _codigo_reservatorio: int,
        _codigo_usina: int,
        _tipo_restricao: str,
        _mes: int,
        _volume_minimo: float,
        _volume_maximo: float,
        _limite_minimo: Optional[float],
        _limite_maximo: Optional[float],
        _periodicidade: str,
        _legenda_faixa: str,
    ):
        self._inicio_vigencia = (
            datetime.fromisoformat(_inicio_vigencia)
            if _inicio_vigencia
            else None
        )
        self._fim_vigencia = (
            datetime.fromisoformat(_fim_vigencia) if _fim_vigencia else None
        )
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
        format = "%Y-%m-%d"
        inicio_vigencia = (
            self._inicio_vigencia.strftime(format)
            if self._inicio_vigencia
            else "-"
        )
        fim_vigencia = (
            self._fim_vigencia.strftime(format) if self._fim_vigencia else "-"
        )
        return (
            f"Regra: reservatório"
            + f" [{inicio_vigencia} -"
            + f" {fim_vigencia}]"
            + f" {self._codigo_reservatorio}"
            + f" -> usina {self._codigo_usina} | {self._tipo_restricao}"
            + f" mês {self._mes}. Faixa {self._legenda_faixa}: "
            + f" ({self._volume_minimo}, {self._volume_maximo})"
            + f" -> ({self._limite_minimo},{self._limite_maximo}). "
            + f" Periodicidade {self._periodicidade}"
        )

    @staticmethod
    def from_csv(caminho: str) -> List["RegraReservatorio"]:
        df = pd.read_csv(caminho, index_col=None, sep=";")
        df = df.loc[~df["INICIO_VIGENCIA"].str.contains("&")]
        regras: List[RegraReservatorio] = []
        for _, linha in df.iterrows():
            regras.append(
                RegraReservatorio(
                    None
                    if linha["INICIO_VIGENCIA"] == "-"
                    else linha["INICIO_VIGENCIA"],
                    None
                    if linha["FIM_VIGENCIA"] == "-"
                    else linha["FIM_VIGENCIA"],
                    int(linha["COD_RESERVATORIO_VOL"]),
                    int(linha["CODIGO_USINA_RESTRICAO"]),
                    linha["TIPO_REST"],
                    int(linha["MES"]),
                    float(linha["VOL_MIN"]),
                    float(linha["VOL_MAX"]),
                    None
                    if linha["LIM_MIN"] == "-"
                    else float(linha["LIM_MIN"]),
                    None
                    if linha["LIM_MAX"] == "-"
                    else float(linha["LIM_MAX"]),
                    linha["PERIOD"],
                    linha["LEGENDA_FAIXA"],
                )
            )
        return regras

    @staticmethod
    def from_json(json_dict: Dict[str, Any]) -> "RegraReservatorio":
        return RegraReservatorio(**json_dict)

    def to_json(self) -> Dict[str, Any]:
        inicio_vigencia = (
            self._inicio_vigencia.isoformat()
            if self._inicio_vigencia
            else None
        )
        fim_vigencia = (
            self._fim_vigencia.isoformat() if self._fim_vigencia else None
        )
        return {
            "_inicio_vigencia": inicio_vigencia,
            "_fim_vigencia": fim_vigencia,
            "_tipo_restricao": self._tipo_restricao,
            "_mes": self._mes,
            "_volume_minimo": self._volume_minimo,
            "_volume_maximo": self._volume_maximo,
            "_limite_minimo": self._limite_minimo,
            "_limite_maximo": self._limite_maximo,
            "_periodicidade": self._periodicidade,
            "_legenda_faixa": self._legenda_faixa,
        }

    @property
    def inicio_vigencia(self) -> Optional[datetime]:
        return self._inicio_vigencia

    @property
    def fim_vigencia(self) -> Optional[datetime]:
        return self._fim_vigencia

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

    @volume_minimo.setter
    def volume_minimo(self, v: float):
        self._volume_minimo = v

    @property
    def volume_maximo(self) -> float:
        return self._volume_maximo

    @volume_maximo.setter
    def volume_maximo(self, v: float):
        self._volume_maximo = v

    @property
    def limite_minimo(self) -> Optional[float]:
        return self._limite_minimo

    @limite_minimo.setter
    def limite_minimo(self, lim: float):
        self._limite_minimo = lim

    @property
    def limite_maximo(self) -> Optional[float]:
        return self._limite_maximo

    @limite_maximo.setter
    def limite_maximo(self, lim: float):
        self._limite_maximo = lim

    @property
    def periodicidade(self) -> str:
        return self._periodicidade

    @property
    def legenda_faixa(self) -> str:
        return self._legenda_faixa
