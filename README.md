# encadeador-pem
Programa de realização e monitoramento de estudos encadeados de NEWAVE / DECOMP.

A versão atual do encadeador suporta apenas o encadeamento dos programas NEWAVE e DECOMP, sendo executados no mesmo cluster, onde o encadeador está localmente instalado. O encadeador é compatível com ambientes Linux, onde esteja instalado `python` na versão 3.8 ou superior. Também é necessário que o sistema operacional possua o pacote para uso do `sqlite3` que, no Ubuntu, já está previamente instalado.

## Dependências

São utilizados diversos serviços e APIs para a execução do estudo encadeado, sendo elas:

- [model-api](https://github.com/rjmalves/hpc-model-api): API para controle e gestão da submissão de execuções de modelos computacionais via sistema de filas.
- [result-api](https://github.com/rjmalves/result-api): API para extração de arquivos estáticos em formato .parquet com aplicação de filtros no lado do servidor.
- [encadeador-service](https://github.com/rjmalves/encadeador-service): serviço para realização do encadeamento entre execuções dos modelos energéticos. Realiza a transferência do estado final de uma execução para o estado inicial de outra.
- [flexibilizador-service](https://github.com/rjmalves/flexibilizador-service): serviço para realização da flexibilização de um caso. Realiza a remoção de inviabilidades por meio da flexiblização de restrições ou outros parâmetros de convergência.
- [regras-operativas-service](https://github.com/rjmalves/regras-operativas-service): serviço para realização da aplicação de regras operativas. Realiza a alteração ou criação de restrições com base em regras operativas, atualmente suportando apenas regras do tipo VOLUME -> DEFLUÊNCIA ou VOLUME -> TURBINAMENTO.


## Configuração

A configuração do encadeador é feita por meio de um arquivo `.env` no diretório de instalação, sendo sobrescrita pela configuração fornecida no arquivo `encadeia.cfg`, existente no diretório de chamada da aplicação. Os campos existentes no arquivo de configuração são:

| Chave | Exemplo | Descrição |
| ----- | ------- | --------- |
| NOME_ESTUDO | "Backtest CPAMP Ciclo X - Caso A" | Nome geral para o estudo encadeado em questão, que será substituído em todos os decks, nos respectivos campos (primeira linha do `dger.dat` e registro `TE` no `dadger.rvN`). |
| NOME_DIRETORIO_NEWAVE | "newave" | Nome da pasta interna a cada diretório "AAAA_MM_rvN" que contém o deck do NEWAVE |
| NOME_DIRETORIO_DECOMP | "decomp" | Nome da pasta interna a cada diretório "AAAA_MM_rvN" que contém o deck do DECOMP |
| VERSAO_NEWAVE | "v28" | Versão do NEWAVE a ser utilizada no estudo |
| VERSAO_DECOMP | "v31" | Versão do DECOMP a ser utilizada no estudo |
| MAXIMO_FLEXIBILIZACOES_REVISAO | 50 | Número máximo de tentativas de flexibilização das restrições de um caso |
| ADEQUA_DECKS_NEWAVE | 1 | Habilita ou não o pré-processamento de decks de NEWAVE para alteração de algumas configurações (atualmente CVaR) |
| CVAR | 25,35 | Valor de CVaR a ser adequado em tempo de execução, caso habilitado |
| ADEQUA_DECKS_DECOMP | 1 | Habilita ou não o pré-processamento de decks de DECOMP para alteração de algumas configurações (atualmente existente o máximo de iterações) |
| MAXIMO_ITERACOES_DECOMP | 500 | Altera o número máximo de iterações permitidas no modelo DECOMP |
| GAP_MAXIMO_DECOMP | 0.1 | O gap máximo permitido quando o modelo não alcança o gap de convergência nas suas iterações, que é aumentado no processo de flexibilização |
| PROCESSADORES_NEWAVE | 64 | Número de processadores utilizados para a execução do NEWAVE |
| PROCESSADORES_DECOMP | 64 | Número de processadores utilizados para a execução do DECOMP |
| VARIAVEIS_ENCADEADAS_NEWAVE | "VARM" | Variáveis a serem encadeadas entre os programas DECOMP e NEWAVE. Suportadas: **VARM, GNL e ENA**. |
| VARIAVEIS_ENCADEADAS_DECOMP | "VARM,TVIAGEM" | Variáveis a serem encadeadas entre os programas DECOMP. Suportadas: **VARM, TVIAGEM, GNL e ENA**. |
| SCRIPT_CONVERTE_CODIFICACAO | "/home/USER/converte.sh" | Script shell para realizar a conversão de arquivos de entrada textuais para UTF-8, eliminando caracteres indesejados. |
| ARQUIVO_LISTA_CASOS | "lista_casos.txt" | Nome do arquivo de entrada que contém os casos a serem encadeados |
| ARQUIVO_REGRAS_OPERACAO_RESERVATORIOS | "regras_reservatorios.csv" | Arquivo com as regras operativas de reservatórios do tipo VOLUME -> DEFLUÊNCIA, se houver. |
| ARQUIVO_REGRAS_FLEXIBILIZACAO_INVIABILIDADES | "regras_inviabilidades.csv" | Arquivo com as regras de flexibilização de restrições em caso de inviabilidade no DECOMP, se houver. |
| MODEL_API | "http://localhost:8080/api/v1/model/" | Endpoint da API utilizada para acesso ao `model-api` |
| RESULT_API | "http://localhost:8080/api/v1/results/results/" | Endpoint da API utilizada para acesso ao `result-api` |
| ENCADEADOR_SERVICE | "http://localhost:8080/api/v1/chain/chain/" | Endpoint da API utilizada para acesso ao `encadeador-service` |
| FLEXIBILIZADOR_SERVICE | "http://localhost:8080/api/v1/flex/flex/" | Endpoint da API utilizada para acesso ao `flexibilizador-service` |
| REGRAS_RESERVATORIOS_SERVICE | "http://localhost:8080/api/v1/rules/reservoir/" | Endpoint da API utilizada para acesso ao `regras-operativas-service` |


## Instalação

Apesar de ser um módulo `python`, o encadeador não está disponível nos repositórios oficiais. Para realizar a instalação, é necessário fazer o download do código a partir do repositório e fazer a instalação manualmente:

```
$ git clone https://github.com/rjmalves/encadeador-pem
$ cd encadeador-pem
$ python setup.py install
```

## Exemplo de uso

Para execução do encadeador, tendo sido feita a instalação, basta chamar a aplicação a partir da linha de comando, estando no diretório onde estão os casos a serem executados, os arquivos `lista_casos.txt` e `encadeia.cfg` e, opcionalmente, os arquivos de regras operativas e de flexibilização de restrições.

```
$ encadeador-pem
> 2023-02-10 02:02:05,214 INFO: Estudo: preparando execução
...
```
