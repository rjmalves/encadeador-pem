import pathlib
import logging
import logging.handlers
from os.path import join
from dotenv import load_dotenv

from encadeador.app import App
from encadeador.modelos.caso import Configuracoes

# Lê as configurações das variáveis de ambiente
load_dotenv(override=True)

DIR_BASE = pathlib.Path().resolve()

load_dotenv(join(DIR_BASE, "encadeia.cfg"), override=True)


def configura_logging() -> logging.Logger:
    root = logging.getLogger()
    h = logging.handlers.RotatingFileHandler(join(DIR_BASE,
                                                  "encadeia.log"),
                                             'a',
                                             10000,
                                             0,
                                             "utf-8")
    f = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    h.setFormatter(f)
    # Logger para STDOUT
    std_h = logging.StreamHandler()
    std_h.setFormatter(f)
    root.addHandler(h)
    root.addHandler(std_h)
    root.setLevel(logging.INFO)
    return root


if __name__ == "__main__":

    log = configura_logging()

    cfg = Configuracoes.le_variaveis_ambiente()
    app = App(cfg, log, DIR_BASE)
    app.executa()
