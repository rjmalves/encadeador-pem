from unittest.mock import patch
from datetime import datetime, timedelta
from typing import List

from encadeador.modelos.job2 import Job
from encadeador.modelos.estadojob import EstadoJob
from encadeador.controladores.gerenciadorfila2 import GerenciadorFilaSGE


@patch(
    "encadeador.controladores.gerenciadorfila2.GerenciadorFilaSGE._processa_sucesso_submissao",
    lambda c: None,
)
def test_comandos_qsub_job_gerenciador_fila_sge():
    j = Job(
        1,
        "Teste",
        "/home/teste",
        datetime.now(),
        datetime.now(),
        datetime.now(),
        72,
        EstadoJob.NAO_INICIADO,
        1,
    )
    g = GerenciadorFilaSGE(j)
    r = g._comando_qsub


@patch(
    "encadeador.controladores.gerenciadorfila2.executa_terminal_retry",
    lambda c: (0, ['Your job 5 ("certo") has been submitted']),
)
def test_inicia_gerenciador_fila_sge():
    j = Job(
        None,
        "errado",
        "/home/teste",
        datetime.now(),
        datetime.now(),
        datetime.now(),
        72,
        EstadoJob.NAO_INICIADO,
        1,
    )
    g = GerenciadorFilaSGE(j)
    r = g.submete()
    assert r
    assert j.nome == "certo"
    assert j.codigo == 5


@patch(
    "encadeador.controladores.gerenciadorfila2.executa_terminal_retry",
    lambda c: (0, []),
)
def test_deleta_gerenciador_fila_sge():
    j = Job(
        None,
        "errado",
        "/home/teste",
        datetime.now(),
        datetime.now(),
        datetime.now(),
        72,
        EstadoJob.NAO_INICIADO,
        1,
    )
    g = GerenciadorFilaSGE(j)
    r = g.deleta()
    assert r


def cria_job(e: EstadoJob, t_entrada=None, t_inicio=None, t_saida=None) -> Job:
    t_inicio = datetime.now() if t_inicio is None else t_inicio
    t_entrada = (
        t_inicio - timedelta(seconds=30) if t_entrada is None else t_entrada
    )
    t_saida = t_inicio + timedelta(seconds=30) if t_saida is None else t_saida
    return Job(
        5,
        "teste1",
        "/home/teste",
        t_entrada,
        t_inicio,
        t_saida,
        10,
        e,
        0,
    )


def gera_job_teste(
    e: EstadoJob, t_entrada=None, t_inicio=None, t_saida=None
) -> Job:
    t_inicio = datetime.now() if t_inicio is None else t_inicio
    t_entrada = (
        t_inicio - timedelta(seconds=30) if t_entrada is None else t_entrada
    )
    t_saida = t_inicio + timedelta(seconds=30) if t_saida is None else t_saida
    return Job(
        5,
        "teste1",
        "/home/teste",
        t_entrada,
        t_inicio,
        t_saida,
        10,
        e,
        0,
    )


def gera_strings_sge_teste(e: str) -> List[str]:
    return [
        "",
        "",
        f"      5 0.00000 teste1     usr          {e.rjust(2)}    06/20/2022 19:55:37                                   72",
    ]


@patch(
    "encadeador.controladores.gerenciadorfila2.executa_terminal_retry",
    lambda c: (0, gera_strings_sge_teste("qw")),
)
def test_monitora_estado_esperando_gerenciador_fila_sge():
    j = gera_job_teste(EstadoJob.NAO_INICIADO)
    g = GerenciadorFilaSGE(j)
    r = g.monitora()
    assert r == EstadoJob.ESPERANDO


@patch(
    "encadeador.controladores.gerenciadorfila2.executa_terminal_retry",
    lambda c: (0, gera_strings_sge_teste("r")),
)
def test_monitora_estado_executando_gerenciador_fila_sge():
    j = gera_job_teste(EstadoJob.NAO_INICIADO)
    g = GerenciadorFilaSGE(j)
    r = g.monitora()
    assert r == EstadoJob.EXECUTANDO


@patch(
    "encadeador.controladores.gerenciadorfila2.executa_terminal_retry",
    lambda c: (0, gera_strings_sge_teste("dr")),
)
def test_monitora_estado_deletando_gerenciador_fila_sge():
    j = gera_job_teste(EstadoJob.NAO_INICIADO)
    g = GerenciadorFilaSGE(j)
    r = g.monitora()
    assert r == EstadoJob.DELETANDO


@patch(
    "encadeador.controladores.gerenciadorfila2.executa_terminal_retry",
    lambda c: (0, gera_strings_sge_teste("de")),
)
def test_monitora_estado_erro_gerenciador_fila_sge():
    j = gera_job_teste(EstadoJob.NAO_INICIADO)
    g = GerenciadorFilaSGE(j)
    r = g.monitora()
    assert r == EstadoJob.ERRO


@patch(
    "encadeador.controladores.gerenciadorfila2.executa_terminal_retry",
    lambda c: (0, gera_strings_sge_teste("r")),
)
@patch(
    "encadeador.controladores.gerenciadorfila2.GerenciadorFila._ultima_modificacao_arquivo",
    lambda c: datetime.now() - timedelta(minutes=40),
)
def test_monitora_estado_timeout_gerenciador_fila_sge():
    j = gera_job_teste(EstadoJob.EXECUTANDO)
    g = GerenciadorFilaSGE(j)
    r = g.monitora()
    assert r == EstadoJob.TIMEOUT


@patch(
    "encadeador.controladores.gerenciadorfila2.executa_terminal_retry",
    lambda c: (0, gera_strings_sge_teste("")),
)
def test_monitora_estado_finalizado_gerenciador_fila_sge():
    j = gera_job_teste(EstadoJob.EXECUTANDO)
    g = GerenciadorFilaSGE(j)
    r = g.monitora()
    assert r == EstadoJob.FINALIZADO
