import pytest

from encadeador.modelos.caso import Caso
from encadeador.modelos.estudo import Estudo
from encadeador.modelos.programa import Programa
from encadeador.modelos.estadocaso import EstadoCaso
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.adapters.repository.caso import SQLCasoRepository
from encadeador.adapters.repository.estudo import SQLEstudoRepository

pytestmark = pytest.mark.usefixtures("mappers")


def test_get_caso_not_found(sqlite_session_factory):
    session = sqlite_session_factory()
    caso_repo = SQLCasoRepository(session)
    assert caso_repo.read(1) is None


def test_get_caso(sqlite_session_factory):
    session = sqlite_session_factory()
    caso_repo = SQLCasoRepository(session)
    estudo_repo = SQLEstudoRepository(session)
    estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
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
    assert caso_repo.read(1) == caso_teste


def test_update_caso(sqlite_session_factory):
    session = sqlite_session_factory()
    caso_repo = SQLCasoRepository(session)
    estudo_repo = SQLEstudoRepository(session)
    estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
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
    caso_lido = caso_repo.read(1)
    caso_lido.caminho = "/home/modificado"
    caso_repo.update(caso_lido)
    assert caso_repo.read(1) == caso_lido


def test_delete_caso(sqlite_session_factory):
    session = sqlite_session_factory()
    caso_repo = SQLCasoRepository(session)
    estudo_repo = SQLEstudoRepository(session)
    estudo_repo.create(Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO))
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
    assert caso_repo.read(1) == caso_teste
    caso_repo.delete(1)
    assert caso_repo.read(1) is None
