import pathlib
import asyncio
from os.path import join
from dotenv import load_dotenv
from config import start_db
from encadeador.app import App
from encadeador.modelos.configuracoes import Configuracoes
from encadeador.utils.log import Log

# Lê as configurações das variáveis de ambiente
load_dotenv(override=True)

DIR_BASE = pathlib.Path().resolve()

load_dotenv(join(DIR_BASE, "encadeia.cfg"), override=True)

if __name__ == "__main__":

    Log.configura_logging(DIR_BASE)
    Configuracoes.le_variaveis_ambiente()
    start_db(DIR_BASE)

    app = App()
    asyncio.run(app.inicializa())
    asyncio.run(app.executa())
