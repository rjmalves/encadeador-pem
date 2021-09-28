import pathlib
from os import chdir
from os.path import join
from logging import getLogger

from encadeador.modelos.configuracoes import Configuracoes
from encadeador.modelos.caso import CasoNEWAVE
from encadeador.modelos.caso import CasoDECOMP
from encadeador.app import App

DIR_INICIAL = pathlib.Path().resolve()
DIR_TESTE = join(DIR_INICIAL, "tests/_arquivos/casos")


log = getLogger()
cfg = Configuracoes()
cfg.diretorio_instalacao_newaves = "../"
cfg.diretorio_instalacao_decomps = "../"
chdir(DIR_TESTE)
a = App(cfg, log)
a.executa()
chdir(DIR_INICIAL)
