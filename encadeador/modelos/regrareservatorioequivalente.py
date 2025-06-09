from typing import List, Optional
import pandas as pd  # type: ignore


class RegraReservatorioEquivalente:
    def __init__(
        self,
        _codigos_reservatorios: List[int],
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
        self._codigos_reservatorios = _codigos_reservatorios
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
            f"Regra: reservatórios {self._codigos_reservatorios}"
            + f" -> usina {self._codigo_usina} | {self._tipo_restricao}"
            + f" mês {self._mes}. Faixa {self._legenda_faixa}: "
            + f" ({self._volume_minimo}, {self._volume_maximo}) [total hm3]"
            + f" -> ({self._limite_minimo},{self._limite_maximo}) [m3/s]. "
            + f" Periodicidade {self._periodicidade}"
        )

    @staticmethod
    def to_csv(regras: List["RegraReservatorioEquivalente"], caminho: str):
        codigos_reservatorios = [str(r.codigos_reservatorios) for r in regras]
        codigo_usina = [r.codigo_usina for r in regras]
        tipo_restricao = [r.tipo_restricao for r in regras]
        mes = [r.mes for r in regras]
        volume_minimo = [r.volume_minimo for r in regras]
        volume_maximo = [r.volume_maximo for r in regras]
        limite_minimo = [r.limite_minimo for r in regras]
        limite_maximo = [r.limite_maximo for r in regras]
        periodicidade = [r.periodicidade for r in regras]
        legenda_faixa = [r._legenda_faixa for r in regras]

        df = pd.DataFrame(
            data={
                "COD_RESERVATORIO_VOL": codigos_reservatorios,
                "CODIGO_USINA_RESTRICAO": codigo_usina,
                "TIPO_REST": tipo_restricao,
                "MES": mes,
                "VOL_MIN": volume_minimo,
                "VOL_MAX": volume_maximo,
                "LIM_MIN": limite_minimo,
                "LIM_MAX": limite_maximo,
                "PERIOD": periodicidade,
                "LEGENDA_FAIXA": legenda_faixa,
            }
        )
        df.to_csv(caminho, sep=";")

    @property
    def codigos_reservatorios(self) -> List[int]:
        return self._codigos_reservatorios

    @codigos_reservatorios.setter
    def codigos_reservatorios(self, c: List[int]):
        self._codigos_reservatorios = c

    @property
    def codigo_usina(self) -> int:
        return self._codigo_usina

    @property
    def tipo_restricao(self) -> str:
        return self._tipo_restricao

    @property
    def mes(self) -> int:
        return self._mes

    @mes.setter
    def mes(self, m: int):
        self._mes = m

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
    def limite_minimo(self, v: float):
        self._limite_minimo = v

    @property
    def limite_maximo(self) -> Optional[float]:
        return self._limite_maximo

    @limite_maximo.setter
    def limite_maximo(self, v: float):
        self._limite_maximo = v

    @property
    def periodicidade(self) -> str:
        return self._periodicidade
