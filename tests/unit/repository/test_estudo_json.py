from unittest.mock import MagicMock, patch, mock_open

from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.modelos.estudo2 import Estudo
from encadeador.adapters.repository.estudo import JSONEstudoRepository
from tests.conftest import JSON_WRITING_FIRST_INDEX


@patch("encadeador.adapters.repository.estudo.exists", lambda p: False)
@patch("builtins.open", mock_open(read_data="[]"))
def test_get_estudo_creates_file_if_not_exists():
    m = MagicMock()
    with patch("encadeador.adapters.repository.estudo.makedirs", m):
        estudo_repo = JSONEstudoRepository(".")
        assert estudo_repo.read(1) is None
        m.assert_called_once()


@patch("encadeador.adapters.repository.estudo.exists", lambda p: True)
@patch("builtins.open", mock_open(read_data="[]"))
def test_get_estudo_not_found():
    estudo_repo = JSONEstudoRepository(".")
    assert estudo_repo.read(1) is None


@patch(
    "encadeador.adapters.repository.caso.JSONCasoRepository.list_by_estudo",
    return_value=[],
)
@patch(
    "encadeador.adapters.repository.estudo.exists",
    return_value=True,
)
def test_get_estudo(ignore_casos_estudo, mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        estudo_repo = JSONEstudoRepository(".")
        estudo_teste = Estudo(
            "/home/teste",
            "teste",
            EstadoEstudo.CONCLUIDO,
        )
        estudo_teste._id = 1
        estudo_repo.create(estudo_teste)
        write_calls = m.mock_calls
        estudo_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=estudo_text)):
        assert estudo_repo.read(1) == estudo_teste


@patch(
    "encadeador.adapters.repository.caso.JSONCasoRepository.list_by_estudo",
    return_value=[],
)
@patch(
    "encadeador.adapters.repository.estudo.exists",
    return_value=True,
)
def test_update_estudo(ignore_casos_estudo, mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        estudo_repo = JSONEstudoRepository(".")
        estudo_teste = Estudo(
            "/home/teste",
            "teste",
            EstadoEstudo.CONCLUIDO,
        )
        estudo_teste._id = 1
        estudo_repo.create(estudo_teste)
        write_calls = m.mock_calls
        estudo_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=estudo_text)):
        estudo_read = estudo_repo.read(1)
        estudo_read.caminho = "/home/modificado"
    m: MagicMock = mock_open(read_data=estudo_text)
    with patch("builtins.open", m):
        estudo_repo.update(estudo_read)
        write_calls = m.mock_calls
        estudo_updated_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=estudo_updated_text)):
        estudo_updated = estudo_repo.read(1)
        assert estudo_updated == estudo_read


@patch(
    "encadeador.adapters.repository.caso.JSONCasoRepository.list_by_estudo",
    return_value=[],
)
@patch(
    "encadeador.adapters.repository.estudo.exists",
    return_value=True,
)
def test_delete_estudo(ignore_casos_estudo, mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        estudo_repo = JSONEstudoRepository(".")
        estudo_teste = Estudo(
            "/home/teste",
            "teste",
            EstadoEstudo.CONCLUIDO,
        )
        estudo_teste._id = 1
        estudo_repo.create(estudo_teste)
        write_calls = m.mock_calls
        estudo_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    m: MagicMock = mock_open(read_data=estudo_text)
    with patch("builtins.open", m):
        estudo_repo.delete(1)
        assert m.mock_calls[JSON_WRITING_FIRST_INDEX].args[0] == "[]"
