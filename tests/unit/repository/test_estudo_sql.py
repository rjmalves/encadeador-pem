import pytest

from encadeador.modelos.estudo2 import Estudo
from encadeador.modelos.estadoestudo import EstadoEstudo
from encadeador.adapters.repository.estudo import SQLEstudoRepository

pytestmark = pytest.mark.usefixtures("mappers")


def test_get_estudo_not_found(sqlite_session_factory):
    session = sqlite_session_factory()
    estudo_repo = SQLEstudoRepository(session)
    assert estudo_repo.read(1) is None


def test_get_estudo(sqlite_session_factory):
    session = sqlite_session_factory()
    estudo_repo = SQLEstudoRepository(session)
    estudo_teste = Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO)
    estudo_teste._id = 1
    estudo_repo.create(estudo_teste)
    assert estudo_repo.read(1) == estudo_teste


def test_update_estudo(sqlite_session_factory):
    session = sqlite_session_factory()
    estudo_repo = SQLEstudoRepository(session)
    estudo_teste = Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO)
    estudo_teste._id = 1
    estudo_repo.create(estudo_teste)
    estudo_lido = estudo_repo.read(1)
    estudo_lido.caminho = "/home/modificado"
    estudo_repo.update(estudo_lido)
    assert estudo_repo.read(1) == estudo_lido


def test_delete_estudo(sqlite_session_factory):
    session = sqlite_session_factory()
    estudo_repo = SQLEstudoRepository(session)
    estudo_teste = Estudo("/home/teste", "teste", EstadoEstudo.CONCLUIDO)
    estudo_teste._id = 1
    estudo_repo.create(estudo_teste)
    estudo_lido = estudo_repo.read(1)
    assert estudo_lido == estudo_repo.read(1)
    estudo_repo.delete(1)
    assert estudo_repo.read(1) is None
