from unittest.mock import MagicMock, patch, mock_open
from datetime import datetime, timedelta

from encadeador.modelos.job2 import Job
from encadeador.modelos.estadojob import EstadoJob
from encadeador.adapters.repository.job import JSONJobRepository
from tests.conftest import JSON_WRITING_FIRST_INDEX


@patch("encadeador.adapters.repository.job.exists", return_value=False)
@patch("builtins.open", mock_open(read_data="[]"))
def test_get_job_creates_file_if_not_exists(mock_exists):
    m = MagicMock()
    with patch("encadeador.adapters.repository.job.makedirs", m):
        job_repo = JSONJobRepository(".")
        assert job_repo.read(1) is None
        m.assert_called_once()


@patch("encadeador.adapters.repository.job.exists", return_value=True)
@patch("builtins.open", mock_open(read_data="[]"))
def test_get_job_not_found(mock_exists):
    job_repo = JSONJobRepository(".")
    assert job_repo.read(1) is None


@patch("encadeador.adapters.repository.job.exists", return_value=True)
def test_get_job(mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        job_repo = JSONJobRepository(".")
        job_teste = Job(
            1,
            "teste",
            "/home/teste",
            datetime.now(),
            datetime.now(),
            datetime.now(),
            72,
            EstadoJob.FINALIZADO,
            1,
        )
        job_teste._id = 1
        job_repo.create(job_teste)
        write_calls = m.mock_calls
        job_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=job_text)):
        assert job_repo.read(1) == job_teste


@patch("encadeador.adapters.repository.job.exists", return_value=True)
def test_update_job(mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        job_repo = JSONJobRepository(".")
        job_teste = Job(
            1,
            "teste",
            "/home/teste",
            datetime.now(),
            datetime.now(),
            datetime.now(),
            72,
            EstadoJob.FINALIZADO,
            1,
        )
        job_teste._id = 1
        job_repo.create(job_teste)
        write_calls = m.mock_calls
        job_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=job_text)):
        job_read = job_repo.read(1)
        job_read._instante_entrada_fila = datetime.now() + timedelta(weeks=1)
    m: MagicMock = mock_open(read_data=job_text)
    with patch("builtins.open", m):
        job_repo.update(job_read)
        write_calls = m.mock_calls
        job_updated_test = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    with patch("builtins.open", mock_open(read_data=job_updated_test)):
        job_updated = job_repo.read(1)
        assert job_updated == job_read


@patch("encadeador.adapters.repository.job.exists", return_value=True)
def test_delete_job(mock_exists):
    m: MagicMock = mock_open(read_data="[]")
    with patch("builtins.open", m):
        job_repo = JSONJobRepository(".")
        job_teste = Job(
            1,
            "teste",
            "/home/teste",
            datetime.now(),
            datetime.now(),
            datetime.now(),
            72,
            EstadoJob.FINALIZADO,
            1,
        )
        job_teste._id = 1
        job_repo.create(job_teste)
        write_calls = m.mock_calls
        job_text = "".join(
            [
                write_calls[i].args[0]
                for i in range(JSON_WRITING_FIRST_INDEX, len(write_calls) - 1)
            ]
        )
    m: MagicMock = mock_open(read_data=job_text)
    with patch("builtins.open", m):
        job_repo.delete(1)
        assert m.mock_calls[JSON_WRITING_FIRST_INDEX].args[0] == "[]"
