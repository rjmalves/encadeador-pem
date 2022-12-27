from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime, timedelta

from encadeador.modelos.rodada import Rodada
from encadeador.modelos.runstatus import RunStatus
from encadeador.adapters.repository.rodada import JSONRodadaRepository
from tests.conftest import JSON_WRITING_FIRST_INDEX


@patch("encadeador.adapters.repository.rodada.exists", return_value=False)
@patch("builtins.open", mock_open(read_data="[]"))
def test_get_rodada_creates_file_if_not_exists(mock_exists):
    m = MagicMock()
    with patch("encadeador.adapters.repository.rodada.makedirs", m):
        rodada_repo = JSONRodadaRepository(".")
        assert rodada_repo.read(1) is None
        m.assert_called_once()


@patch("encadeador.adapters.repository.rodada.exists", return_value=True)
@patch("builtins.open", mock_open(read_data="[]"))
def test_get_rodada_not_found(mock_exists):
    rodada_repo = JSONRodadaRepository(".")
    assert rodada_repo.read(1) is None


@patch("encadeador.adapters.repository.rodada.exists", return_value=True)
def test_get_rodada(mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        rodada_repo = JSONRodadaRepository(".")
        rodada_teste = Rodada(
            "teste",
            RunStatus.SUCCESS,
            1,
            "/home/teste",
            datetime.now(),
            datetime.now(),
            72,
            "NEWAVE",
            "v28",
            1,
        )
        rodada_teste.id = 1
        rodada_repo.create(rodada_teste)
        write_calls = m.mock_calls
        rodada_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=rodada_text)):
        assert rodada_repo.read(1) == rodada_teste


@patch("encadeador.adapters.repository.rodada.exists", return_value=True)
def test_update_rodada(mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        rodada_repo = JSONRodadaRepository(".")
        rodada_teste = Rodada(
            "teste",
            RunStatus.SUCCESS,
            1,
            "/home/teste",
            datetime.now(),
            datetime.now(),
            72,
            "NEWAVE",
            "v28",
            1,
        )
        rodada_teste.id = 1
        rodada_repo.create(rodada_teste)
        write_calls = m.mock_calls
        rodada_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=rodada_text)):
        rodada_read = rodada_repo.read(1)
        rodada_read.instante_inicio_execucao = datetime.now() + timedelta(
            weeks=1
        )
    m: MagicMock = mock_open(read_data=rodada_text)
    with patch("builtins.open", m):
        rodada_repo.update(rodada_read)
        write_calls = m.mock_calls
        rodada_updated_test = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=rodada_updated_test)):
        rodada_updated = rodada_repo.read(1)
        assert rodada_updated == rodada_read


@patch("encadeador.adapters.repository.rodada.exists", return_value=True)
def test_delete_rodada(mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        rodada_repo = JSONRodadaRepository(".")
        rodada_teste = Rodada(
            "teste",
            RunStatus.SUCCESS,
            1,
            "/home/teste",
            datetime.now(),
            datetime.now(),
            72,
            "NEWAVE",
            "v28",
            1,
        )
        rodada_teste.id = 1
        rodada_repo.create(rodada_teste)
        write_calls = m.mock_calls
        rodada_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    m: MagicMock = mock_open(read_data=rodada_text)
    with patch("builtins.open", m):
        rodada_repo.delete(1)
        assert m.mock_calls[JSON_WRITING_FIRST_INDEX].args[0] == "[]"
