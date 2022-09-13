from unittest.mock import MagicMock, patch, mock_open

from encadeador.modelos.caso import Caso
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.adapters.repository.caso import JSONCasoRepository
from encadeador.modelos.programa import Programa
from tests.conftest import JSON_WRITING_FIRST_INDEX


@patch("encadeador.adapters.repository.caso.exists", lambda p: False)
@patch("builtins.open", mock_open(read_data="[]"))
def test_get_caso_creates_file_if_not_exists():
    m = MagicMock()
    with patch("encadeador.adapters.repository.caso.makedirs", m):
        caso_repo = JSONCasoRepository(".")
        assert caso_repo.read(1) is None
        m.assert_called_once()


@patch("encadeador.adapters.repository.caso.exists", lambda p: True)
@patch("builtins.open", mock_open(read_data="[]"))
def test_get_caso_not_found():
    caso_repo = JSONCasoRepository(".")
    assert caso_repo.read(1) is None


@patch(
    "encadeador.adapters.repository.job.JSONJobRepository.list_by_caso",
    return_value=[],
)
@patch(
    "encadeador.adapters.repository.caso.exists",
    return_value=True,
)
def test_get_caso(ignore_jobs_caso, mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        caso_repo = JSONCasoRepository(".")
        caso_teste = Caso(
            "/home/teste",
            "teste",
            2020,
            1,
            0,
            Programa.NEWAVE,
            EstadoCaso.CONCLUIDO,
            1,
        )
        caso_teste._id = 1
        caso_repo.create(caso_teste)
        write_calls = m.mock_calls
        caso_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=caso_text)):
        assert caso_repo.read(1) == caso_teste


@patch(
    "encadeador.adapters.repository.job.JSONJobRepository.list_by_caso",
    return_value=[],
)
@patch(
    "encadeador.adapters.repository.caso.exists",
    return_value=True,
)
def test_update_caso(ignore_jobs_caso, mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        caso_repo = JSONCasoRepository(".")
        caso_teste = Caso(
            "/home/teste",
            "teste",
            2020,
            1,
            0,
            Programa.NEWAVE,
            EstadoCaso.CONCLUIDO,
            1,
        )
        caso_teste._id = 1
        caso_repo.create(caso_teste)
        write_calls = m.mock_calls
        caso_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=caso_text)):
        caso_read = caso_repo.read(1)
        caso_read.caminho = "/home/modificado"
    m: MagicMock = mock_open(read_data=caso_text)
    with patch("builtins.open", m):
        caso_repo.update(caso_read)
        write_calls = m.mock_calls
        caso_updated_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=caso_updated_text)):
        caso_updated = caso_repo.read(1)
        assert caso_updated == caso_read


@patch(
    "encadeador.adapters.repository.job.JSONJobRepository.list_by_caso",
    return_value=[],
)
@patch(
    "encadeador.adapters.repository.caso.exists",
    return_value=True,
)
def test_delete_caso(ignore_jobs_caso, mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        caso_repo = JSONCasoRepository(".")
        caso_teste = Caso(
            "/home/teste",
            "teste",
            2020,
            1,
            0,
            Programa.NEWAVE,
            EstadoCaso.CONCLUIDO,
            1,
        )
        caso_teste._id = 1
        caso_repo.create(caso_teste)
        write_calls = m.mock_calls
        caso_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    m: MagicMock = mock_open(read_data=caso_text)
    with patch("builtins.open", m):
        caso_repo.delete(1)
        assert m.mock_calls[JSON_WRITING_FIRST_INDEX].args[0] == "[]"
