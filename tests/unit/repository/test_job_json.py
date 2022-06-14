from unittest.mock import MagicMock, patch, mock_open

from encadeador.modelos.job2 import Job
from encadeador.modelos.caso2 import Caso
from encadeador.modelos.estudo2 import Estudo
from encadeador.modelos.programa import Programa
from encadeador.modelos.estadojob import EstadoJob
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.adaptadores.repository.job import JSONJobRepository
from encadeador.adaptadores.repository.caso import SQLCasoRepository
from encadeador.adaptadores.repository.estudo import SQLEstudoRepository


def test_get_job_creates_file_if_not_exists():
    job_repo = JSONJobRepository(".")
    with patch(
        "encadeador.adaptadores.repository.job.exists", lambda p: False
    ):
        with patch("builtins.open", mock_open(read_data="[]")):
            assert job_repo.read(1) is None


def test_get_job_not_found():
    job_repo = JSONJobRepository(".")
    with patch("encadeador.adaptadores.repository.job.exists", lambda p: True):
        with patch("builtins.open", mock_open(read_data="[]")):
            assert job_repo.read(1) is None


def test_get_job():
    pass
    # session = sqlite_session_factory()
    # job_repo = SQLJobRepository(session)
    # caso_repo = SQLCasoRepository(session)
    # estudo_repo = SQLEstudoRepository(session)
    # estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
    # caso_repo.create(
    #     Caso(
    #         "/home/teste",
    #         "teste",
    #         2020,
    #         1,
    #         0,
    #         Programa.NEWAVE,
    #         EstadoCaso.CONCLUIDO,
    #         1,
    #     )
    # )
    # job_teste = Job(
    #     "teste", "/home/teste", 0, 10, 50, 72, EstadoJob.FINALIZADO, 1
    # )
    # job_teste._id = 1
    # job_repo.create(job_teste)
    # assert job_repo.read(1) == job_teste


def test_update_job():
    pass
    # session = sqlite_session_factory()
    # job_repo = SQLJobRepository(session)
    # caso_repo = SQLCasoRepository(session)
    # estudo_repo = SQLEstudoRepository(session)
    # estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
    # caso_repo.create(
    #     Caso(
    #         "/home/teste",
    #         "teste",
    #         2020,
    #         1,
    #         0,
    #         Programa.NEWAVE,
    #         EstadoCaso.CONCLUIDO,
    #         1,
    #     )
    # )
    # job_teste = Job(
    #     "teste", "/home/teste", 0, 10, 50, 72, EstadoJob.FINALIZADO, 1
    # )
    # job_teste._id = 1
    # job_repo.create(job_teste)
    # job_lido = job_repo.read(1)
    # assert job_lido == job_teste
    # job_lido._instante_entrada_fila = 5
    # job_repo.update(job_lido)
    # assert job_repo.read(1) == job_lido


def test_delete_job():
    pass
    # session = sqlite_session_factory()
    # job_repo = SQLJobRepository(session)
    # caso_repo = SQLCasoRepository(session)
    # estudo_repo = SQLEstudoRepository(session)
    # estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
    # caso_repo.create(
    #     Caso(
    #         "/home/teste",
    #         "teste",
    #         2020,
    #         1,
    #         0,
    #         Programa.NEWAVE,
    #         EstadoCaso.CONCLUIDO,
    #         1,
    #     )
    # )
    # job_teste = Job(
    #     "teste", "/home/teste", 0, 10, 50, 72, EstadoJob.FINALIZADO, 1
    # )
    # job_teste._id = 1
    # job_repo.create(job_teste)
    # assert job_repo.read(1) == job_teste
    # job_repo.delete(1)
    # assert job_repo.read(1) is None
