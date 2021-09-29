import pytest
from pytest_mock.plugin import MockerFixture

from encadeador.modelos.estadojob import EstadoJob
from encadeador.controladores.gerenciadorfila import GerenciadorFila
from encadeador.controladores.gerenciadorfila import GerenciadorFilaSGE


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


@pytest.fixture
def mock_executa_terminal(mocker: MockerFixture):
    return mocker.patch("encadeador.controladores.gerenciadorfila" +
                        ".executa_terminal")


def test_sge_agenda_job(mock_executa_terminal: MockerFixture):
    g = GerenciadorFilaSGE()
    str_submit = 'Your job 123 ("pmo") has been submitted'
    mock_executa_terminal.return_value = (0, [str_submit])
    g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                 "nw_teste",
                 72)
    assert g.nome_job == "pmo"
    assert g.id_job == 123
    assert g.arquivo_stdout == "pmo.o123"
    assert g.arquivo_stderr == "pmo.e123"


def test_sge_estado_job_fila_qw(mock_executa_terminal: MockerFixture):
    g = GerenciadorFilaSGE()
    str_submit = 'Your job 123 ("pmo") has been submitted'
    mock_executa_terminal.return_value = (0, [str_submit])
    g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                 "nw_teste",
                 72)
    str_stat = ["", "",
                "123 0.00000 pmo        pem          qw    09/22/2021" +
                " 13:17:19                                   72           "]
    mock_executa_terminal.return_value = (0, str_stat)
    assert g.estado_job == EstadoJob.ESPERANDO


def test_sge_estado_job_fila_t(mock_executa_terminal: MockerFixture):
    g = GerenciadorFilaSGE()
    str_submit = 'Your job 123 ("pmo") has been submitted'
    mock_executa_terminal.return_value = (0, [str_submit])
    g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                 "nw_teste",
                 72)
    str_stat = ["", "",
                "123 0.00000 pmo        pem          t     09/22/2021" +
                " 13:17:19                                   72           "]
    mock_executa_terminal.return_value = (0, str_stat)
    assert g.estado_job == EstadoJob.ESPERANDO


def test_sge_estado_job_fila_executando(mock_executa_terminal: MockerFixture):
    g = GerenciadorFilaSGE()
    str_submit = 'Your job 123 ("pmo") has been submitted'
    mock_executa_terminal.return_value = (0, [str_submit])
    g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                 "nw_teste",
                 72)
    str_stat = ["", "",
                "123 0.00000 pmo        pem          r     09/22/2021" +
                " 13:17:19                                   72           "]
    mock_executa_terminal.return_value = (0, str_stat)
    assert g.estado_job == EstadoJob.EXECUTANDO


def test_sge_estado_job_deletando(mock_executa_terminal: MockerFixture):
    g = GerenciadorFilaSGE()
    str_submit = 'Your job 123 ("pmo") has been submitted'
    mock_executa_terminal.return_value = (0, [str_submit])
    g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                 "nw_teste",
                 72)
    str_stat = ["", "",
                "123 0.00000 pmo        pem          dr    09/22/2021" +
                " 13:17:19                                   72           "]
    mock_executa_terminal.return_value = (0, str_stat)
    assert g.estado_job == EstadoJob.DELETANDO


def test_sge_estado_job_fila_erro(mock_executa_terminal: MockerFixture):
    g = GerenciadorFilaSGE()
    str_submit = 'Your job 123 ("pmo") has been submitted'
    mock_executa_terminal.return_value = (0, [str_submit])
    g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                 "nw_teste",
                 72)
    str_stat = ["", "",
                "123 0.00000 pmo        pem          de    09/22/2021" +
                " 13:17:19                                   72           "]
    mock_executa_terminal.return_value = (0, str_stat)
    assert g.estado_job == EstadoJob.ERRO


def test_sge_deleta_job(mock_executa_terminal: MockerFixture):
    g = GerenciadorFilaSGE()
    str_submit = 'Your job 123 ("pmo") has been submitted'
    mock_executa_terminal.return_value = (0, [str_submit])
    g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                 "nw_teste",
                 72)
    str_delete = "pem has deleted job 411"
    mock_executa_terminal.return_value = (0, [str_delete])
    r = g.deleta_job()
    assert r


def test_sge_erro_agenda_caso(mock_executa_terminal: MockerFixture):
    with pytest.raises(IndexError):
        g = GerenciadorFilaSGE()
        mock_executa_terminal.return_value = (1, ["", "", ""])
        g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                     "nw_teste",
                     72)


def test_sge_erro_estado_caso(mock_executa_terminal: MockerFixture):
    with pytest.raises(KeyError):
        g = GerenciadorFilaSGE()
        str_submit = 'Your job 123 ("pmo") has been submitted'
        mock_executa_terminal.return_value = (0, [str_submit])
        g.agenda_job("./tests/_arquivos/newave/mpi_newave_test.job",
                     "nw_teste",
                     72)
        mock_executa_terminal.return_value = (0, ["", "", ""])
        g.estado_job
