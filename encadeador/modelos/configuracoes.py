class Configuracoes:
    def __init__(self) -> None:
        self.nome_estudo = "Backtest CPAMP 2015-2021 CVAR AxL"
        self.diretorio_newave = "newave"
        self.versao_newave = "newave"
        self.diretorio_instalacao_newaves = "./tests/_arquivos"
        self.processadores_minimos_newave = 72
        self.processadores_maximos_newave = 72
        self.ajuste_processadores_newave = False
        self.diretorio_decomp = "decomp"
        self.versao_decomp = "decomp"
        self.diretorio_instalacao_decomps = "./tests/_arquivos"
        self.processadores_minimos_decomp = 72
        self.processadores_maximos_decomp = 72
        self.ajuste_processadores_decomp = False
        self.gerenciador_fila = "SGE"
        self.adequa_decks_newave = True
        self.cvar = (50, 35)
        self.parpa = 3
        self.adequa_decks_decomp = True
        self.maximo_iteracoes_decomp = 500
        self.fator_aumento_gap_decomp = 10
        self.gap_maximo_decomp = 1e-1
        self.arquivo_lista_casos = "lista_casos.txt"
        self.max_flex_decomp = 3

    @classmethod
    def le_variaveis_ambiente(cls) -> 'Configuracoes':
        return Configuracoes()
