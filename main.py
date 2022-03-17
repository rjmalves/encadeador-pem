import pathlib
from os.path import join
from dotenv import load_dotenv

from encadeador.app import App
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.utils.log import Log

print("teste")

# Lê as configurações das variáveis de ambiente
load_dotenv(override=True)

DIR_BASE = pathlib.Path().resolve()

load_dotenv(join(DIR_BASE, "encadeia.cfg"), override=True)

if __name__ == "__main__":

    Log.configura_logging(DIR_BASE)
    Configuracoes.le_variaveis_ambiente()

    app = App()
    app.executa()
