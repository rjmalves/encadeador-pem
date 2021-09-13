import glob

def LeLimInf(string):
    aux = string.strip()
    if aux == "":
        valor = 0
    else:
        aux = aux.partition('.')
        if not aux[-1].isdigit():
            valor = float(aux[0])
        else:
            valor = float(string)
    return valor

def LeLimSup(string):
    aux = string.strip()
    if aux == "":
        valor = 9999999
    else:
        aux = aux.partition('.')
        if not aux[-1].isdigit():
            valor = float(aux[0])
        else:
            valor = float(string)
    return valor


class cDADOS_RESTRICAO():
    def __init__(self, num_estagio):
        # valores inferiores das restricoes
        self.valores_inf = [[0]*3 for i in range(num_estagio)]
        # valores superiores das restricoes
        self.valores_sup = [[0]*3 for i in range(num_estagio)]
        # demais linhas da restricao (as que nao envolvem numeros mas sao necessarias)
        self.linhas = []


    def flexDados(self, etapa, patamar, tresPat, tipo_limite, valor):
        # se eh limite inferior
        if tipo_limite == 0:
            for ipat in range(3):
                if (patamar == ipat + 1) or tresPat:
                    self.valores_inf[etapa-1][ipat] = max(self.valores_inf[etapa-1][ipat] - valor, 0)
        # se eh limite superior
        else:
            for ipat in range(3):
                if (patamar == ipat + 1) or tresPat:
                    self.valores_sup[etapa-1][ipat] = min(self.valores_sup[etapa-1][ipat] + valor, 9999999)

    def getDadosREHQ(self, linha_achada, num_estagio):
        try:
            arquivo = glob.glob('dadger.*')
            with open(arquivo[0], 'r') as arqDADGER:
                # primeiro procura a restricao
                while True:
                    linha = arqDADGER.readline()
                    if linha == linha_achada:
                        break
                # depois procura a sequencia de LUs, FUs, FTs, FIs
                while linha[0:2] not in ["LU", "FU", "FT", "FI", "LQ", "CQ"]:
                    linha = arqDADGER.readline()
                # obtem as informacoes
                while linha[0:2] in ["LU", "FU", "FT", "FI", "LQ", "CQ"]:
                    # se achar LU, pega os numeros e completa do estagio lido ate o final
                    if linha[0:2] in ["LU", "LQ"]:
                        estagio_lido = int(linha[9:13].strip())
                        for iest in range(estagio_lido-1, num_estagio):
                            for ipat in range(3):
                                self.valores_inf[iest][ipat] = LeLimInf(linha[14 + 20*ipat : 24 + 20*ipat])
                                self.valores_sup[iest][ipat] = LeLimSup(linha[24 + 20*ipat : 34 + 20*ipat])
                    # caso contrario, adiciona a lista de linhas uteis da restricao
                    else:
                        self.linhas.append(linha)
                    linha = arqDADGER.readline()
        except IOError:
            print('Arquivo DADGER.* nao encontrado')

    def getDadosHVHA(self, linha_achada, num_estagio):
        try:
            arquivo = glob.glob('dadger.*')
            with open(arquivo[0], 'r') as arqDADGER:
                # primeiro procura a restricao
                while True:
                    linha = arqDADGER.readline()
                    if linha == linha_achada:
                        break
                # depois procura a sequencia de LAs, CAs ou LVs, CVs
                while linha[0:2] not in ["LA", "CA", "LV", "CV"]:
                    linha = arqDADGER.readline()
                # obtem as informacoes
                while linha[0:2] in ["LA", "CA", "LV", "CV"]:
                    # se achar LU, pega os numeros e completa do estagio lido ate o final
                    if linha[0:2] in ["LA", "LV"]:
                        estagio_lido = int(linha[9:13].strip())
                        for iest in range(estagio_lido-1, num_estagio):
                            self.valores_inf[iest][0] = LeLimInf(linha[14:24])
                            self.valores_sup[iest][0] = LeLimSup(linha[24:34])
                    # caso contrario, adiciona a lista de linhas uteis da restricao
                    else:
                        self.linhas.append(linha)
                    linha = arqDADGER.readline()
        except IOError:
            print('Arquivo DADGER.* nao encontrado')