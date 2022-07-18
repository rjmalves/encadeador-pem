# encadeador-pem
Programa de realização e monitoramento de estudos encadeados de NEWAVE / DECOMP.

| Chave | Exemplo | Descrição |
| ----- | ------- | --------- |
| NOME_ESTUDO | "Backtest CPAMP Ciclo X - Caso A" | Nome geral para o estudo encadeado em questão, que será substituído em todos os decks, nos respectivos campos (primeira linha do `dger.dat` e registro `TE` no `dadger.rvN`). |
| ARQUIVO_LISTA_CASOS | "lista_casos.txt" | Nome do arquivo de entrada que contém os casos a serem encadeados |
| NOME_DIRETORIO_NEWAVE | "newave" | Nome da pasta interna a cada diretório "AAAA_MM_rvN" que contém o deck do NEWAVE |
| NOME_DIRETORIO_DECOMP | "decomp" | Nome da pasta interna a cada diretório "AAAA_MM_rvN" que contém o deck do DECOMP |
| VERSAO_NEWAVE | "v28" | Versão do NEWAVE a ser utilizada no estudo |
| VERSAO_DECOMP | "v31" | Versão do DECOMP a ser utilizada no estudo |
| FLEXIBILIZA_DEFICIT | 0 | Realiza a flexibilização de restrições RHE quando a única inviabilidade do caso for a existência de déficit |
| MAXIMO_FLEXIBILIZACOES_REVISAO | 50 | Número máximo de tentativas de flexibilização das restrições de um caso |
| METODO_FLEXIBILIZACAO | "absoluto" | Método de flexibilização das restrições para remoção de inviabilidades. O "absoluto" significa o montante absoluto da restrição adicionado de uma tolerância |
| ADEQUA_DECKS_NEWAVE | 1 | Habilita ou não o pré-processamento de decks de NEWAVE para alteração de algumas configurações (atualmente existente CVaR e PAR(p)-A) |
| CVAR | 25,35 | Valor de CVaR a ser adequado em tempo de execução, caso habilitado |
| OPCAO_PARPA | 3,0 | Opção do modelo de geração de cenários de afluência a ser adequado em tempo de execução, caso habilitado |
| ADEQUA_DECKS_DECOMP | 1 | Habilita ou não o pré-processamento de decks de DECOMP para alteração de algumas configurações (atualmente existente a prevenção do gap negativo por meio dos registros `RT CRISTA` e `RT DESVIO`) |
| PREVINE_GAP_NEGATIVO | 0 | Realiza a prevenção de gap negativo, se habilitado |
| VARIAVEIS_ENCADEADAS | "EARM,TVIAGEM" | Variáveis a serem encadeadas entre os programas DECOMP e NEWAVE. Suportadas: **EARM, TVIAGEM, GNL e ENA**. |
| ARQUIVO_REGRAS_OPERACAO_RESERVATORIOS | "regras_reservatorios.csv" | Arquivo com as regras operativas de reservatórios do tipo VOLUME -> DEFLUÊNCIA, se houver. |

