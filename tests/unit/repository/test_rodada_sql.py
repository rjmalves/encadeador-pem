import pytest
from datetime import datetime, timedelta

from encadeador.modelos.rodada import Rodada
from encadeador.modelos.caso import Caso
from encadeador.modelos.estudo import Estudo
from encadeador.modelos.programa import Programa
from encadeador.modelos.runstatus import RunStatus
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.adapters.repository.rodada import SQLRodadaRepository
from encadeador.adapters.repository.caso import SQLCasoRepository
from encadeador.adapters.repository.estudo import SQLEstudoRepository

pytestmark = pytest.mark.usefixtures("mappers")


def test_get_rodada_not_found(sqlite_session_factory):
    session = sqlite_session_factory()
    rodada_repo = SQLRodadaRepository(session)
    assert rodada_repo.read(1) is None


def test_get_rodada(sqlite_session_factory):
    session = sqlite_session_factory()
    rodada_repo = SQLRodadaRepository(session)
    caso_repo = SQLCasoRepository(session)
    estudo_repo = SQLEstudoRepository(session)
    estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
    caso_repo.create(
        Caso(
            "/home/teste",
            "teste",
            2020,
            1,
            0,
            Programa.NEWAVE,
            EstadoCaso.CONCLUIDO,
            1,
        )
    )
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
    assert rodada_repo.read(1) == rodada_teste


def test_update_rodada(sqlite_session_factory):
    session = sqlite_session_factory()
    rodada_repo = SQLRodadaRepository(session)
    caso_repo = SQLCasoRepository(session)
    estudo_repo = SQLEstudoRepository(session)
    estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
    caso_repo.create(
        Caso(
            "/home/teste",
            "teste",
            2020,
            1,
            0,
            Programa.NEWAVE,
            EstadoCaso.CONCLUIDO,
            1,
        )
    )
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
    rodada_lida = rodada_repo.read(1)
    assert rodada_lida == rodada_teste
    rodada_lida.instante_inicio_execucao = datetime.now() + timedelta(weeks=1)
    rodada_repo.update(rodada_lida)
    assert rodada_repo.read(1) == rodada_lida


def test_delete_rodada(sqlite_session_factory):
    session = sqlite_session_factory()
    rodada_repo = SQLRodadaRepository(session)
    caso_repo = SQLCasoRepository(session)
    estudo_repo = SQLEstudoRepository(session)
    estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
    caso_repo.create(
        Caso(
            "/home/teste",
            "teste",
            2020,
            1,
            0,
            Programa.NEWAVE,
            EstadoCaso.CONCLUIDO,
            1,
        )
    )
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
    assert rodada_repo.read(1) == rodada_teste
    rodada_repo.delete(1)
    assert rodada_repo.read(1) is None
