import pytest
from pytest_mock.plugin import MockerFixture

from encadeador.modelos.gerenciadorfila import GerenciadorFila
from encadeador.modelos.gerenciadorfila import GerenciadorFilaSGE


def test_gerenciador_nao_inicializado_id():
    with pytest.raises(ValueError):
        g = GerenciadorFilaSGE()
        g.id_job


def test_gerenciador_nao_inicializado_nome():
    with pytest.raises(ValueError):
        g = GerenciadorFilaSGE()
        g.nome_job


def test_gerenciador_nao_inicializado_stdout():
    with pytest.raises(ValueError):
        g = GerenciadorFilaSGE()
        g.arquivo_stdout


def test_gerenciador_nao_inicializado_stderr():
    with pytest.raises(ValueError):
        g = GerenciadorFilaSGE()
        g.arquivo_stderr


def test_factory_sge():
    g = GerenciadorFila.factory("SGE")
    assert type(g) == GerenciadorFilaSGE


def test_sge_agenda_job(mocker: MockerFixture):
    g = GerenciadorFilaSGE()
    str_submit = 'Your job 123 ("pmo") has been submitted'
    mocker.patch("encadeador.utils.terminal.executa_terminal",
                 return_value=(0, [str_submit]))
    g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                 "nw_teste",
                 72)
