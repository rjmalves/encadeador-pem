# -*- coding: latin-1 -*-

"""
    rotinas responsaveis pela retirada das inviabilidades
"""

from trataInviab import *
from inviab import *
from dados_restricao import *
import funcoes
import importaHidr
import math
import sys

import funcoes
import importaRelato
import importaRelato2

# funcao de chamada para retirar as inviabilidades do DECOMP
def retiraInviab(inviab, regras, iteracao, usinas):

    valida = True

    # abre o arquivo de log, para informar o andamento do processo
    with open('retirainviab.log', 'a') as arqRetInviab:
        # informa que esta na etapa de retirada das inviabilidades
        arqRetInviab.write('\nINICIO DA ETAPA DE RETIRAR AS INVIABILIDADES\n')

    duracao = funcoes.cDADGER()
    duracao.obtemDuracaoPatamares()

    # AL - obtem numero de estagios do DECOMP
    num_estagio = getNumEstagio()

    #varre todas as inviabilidades
    for index in range(len(inviab.listaTipo)):
        # variavel que informa se o valor foi flexibilizado
        flex = False

        # obtem o indice da regra para retirar essa inviabilidade
        indice = getIndiceRegra(inviab, index, regras, iteracao)

        #verifica se achou a regra de flexibilizacao
        if indice > -1:
            # calcula o valor que deve ser retirado (faz as equivalencias de unidades vazao -> volume, vazao -> geracao, etc...)
            # AL - recebeu "num_estagio" a mais
            
            
            valor = getValorFlexibilizar(inviab, index, regras, indice, duracao, usinas, num_estagio)
            #procura no DADGER a restricao que deve flexibilizar e flexibiliza
            # AL - recebeu "num_estagio" a mais
            flex = flexibilizaDADGER(inviab, index, regras, indice, valor, usinas, iteracao, num_estagio)

        # se nao for informa o erro no log
        else:
            # abre o arquivo de log, para informar o erro
            with open('retirainviab.log', 'a') as arqRetInviab:
                # informa que nao achou a regra
                if indice == -1:
                    arqRetInviab.write('ERRO: regra de flexibilizacao da ' + inviab.listaTipo[index] + ' ' + str("%3i" % (inviab.listaCod[index][0])) + ' etapa ' + str(inviab.listaEtapa[index]) + ' nao encontrada, o processo sera interrompido\n')
                    valida = False
                elif indice == -2:                  
                    arqRetInviab.write('ATENCAO: regra de flexibilizacao da ' + inviab.listaTipo[index] + ' ' + str("%3i" % (inviab.listaCod[index][0])) + ' etapa ' + str(inviab.listaEtapa[index]) + ' iteracao ' + str(iteracao) + ' indica flexibilizacao em iteracoes futuras\n')

    return valida

# funcao que retorna o indice da regra a ser flexibilizada
def getIndiceRegra(inviab, indiceInviab, regras, iteracao):

    etapa = 0
    resp = -1
    iterAnterior=0
    # varre todas as regras
    for indice in range(len(regras.listaTipoRestr)):
        #verifica se eh a regra correta
        # se for o mesmo tipo
        if regras.listaTipoRestr[indice] == inviab.listaTipo[indiceInviab]:
            # se for o mesmo codigo
            if (regras.listaCodRestr[indice] == inviab.listaCod[indiceInviab][0]) or (regras.listaCodRestr[indice] == 0):
                # se for no mesmo limite (superior ou inferior)
                if regras.listaLimite[indice] == inviab.listaLimite[indiceInviab]:
                    # se for estiver na mesma etapa ou em uma anterior
                    if (regras.listaEtapaRestr[indice] <= inviab.listaEtapa[indiceInviab]) and (etapa <= inviab.listaEtapa[indiceInviab]):
                        # se chegou ate esta etapa signica que a regra existe porem, talvez em outra iteracao
                        if resp == -1:
                            resp = -2
                        # se for na mesma iteracao ou em uma menor
                        if (regras.listaIteracao[indice] <= iteracao and iterAnterior <= regras.listaIteracao[indice]):
                            resp = indice
                            etapa = regras.listaEtapaRestr[indice]
                            iterAnterior = regras.listaIteracao[indice]

    return resp

# funcao que retorna o valor a ser flexibilizado, fazendo as equivalencias entre vazao<->volume, vazao<->geracao e volume<->geracao
# AL - recebeu "num_estagio" a mais
def getValorFlexibilizar(inviab, index, regras, indice, duracao, usinas_hidr, num_estagio):

    # se as duas forem de vazao, retorna o proprio valor
    if inviab.listaTipo[index] == 'TI' and regras.listaTipoRestrFlex[indice] == 'HQ':
        valor = round(inviab.listaValor[index],1) + 0.1
    elif inviab.listaTipo[index] == 'HQ' and regras.listaTipoRestrFlex[indice] == 'TI':
        valor = round(inviab.listaValor[index],2) + 0.01
    # se for geracao para vazao
    elif inviab.listaTipo[index] == 'RE' and (regras.listaTipoRestr[indice] == 'TI' or regras.listaTipoRestrFlex[indice] == 'HQ'):

        relato1 = glob.glob('relato.*')
        relato2 = glob.glob('relato2.*')

        prodt = 1

        i = int(regras.listaUsinaEnvolvida[indice])

        nome_usina = str(usinas_hidr[i].nome)

        # AL - Assume DECOMP estocastico apenas no ultimo estagio
        if inviab.listaEtapa[index] < num_estagio:

            prodt = importaRelato.c_relato().leRelato(relato1[0], nome_usina)

        # AL - Assume DECOMP estocastico apenas no ultimo estagio
        elif inviab.listaEtapa[index] == num_estagio:

            prodt = importaRelato2.c_relato2().leRelato2(relato2[0], nome_usina, inviab.listaCenario[index])

        valor = inviab.listaValor[index]/prodt

        if regras.listaTipoRestrFlex[indice] == 'TI':
            valor = round(valor,2) + 0.01
        elif regras.listaTipoRestrFlex[indice] == 'HQ':
            valor = round(valor,1) + 0.1

    # se for vazao para geracao
    elif (inviab.listaTipo[index] == 'TI' or inviab.listaTipo[index] == 'HQ') and regras.listaTipoRestrFlex[indice] == 'RE':

        relato1 = glob.glob('relato.*')
        relato2 = glob.glob('relato2.*')

        prodt = 1

        i = int(regras.listaUsinaEnvolvida[indice])

        nome_usina = str(usinas_hidr[i].nome)

        # AL - Assume DECOMP estocastico apenas no ultimo estagio
        if inviab.listaEtapa[index] < num_estagio:

            prodt = importaRelato.c_relato().leRelato(relato1[0], nome_usina)

        # AL - Assume DECOMP estocastico apenas no ultimo estagio
        elif inviab.listaEtapa[index] == num_estagio:

            prodt = importaRelato2.c_relato2().leRelato2(relato2[0], nome_usina, inviab.listaCenario[index])

        valor = round(inviab.listaValor[index]*prodt,1) + 0.1

    #se for vazao para volume
    elif inviab.listaTipo[index] == 'TI' and regras.listaTipoRestrFlex[indice] == 'HV':
        
        # passa de m3/s para hm3
        valor = inviab.listaValor[index]*(60*60*duracao.total)*0.000001

        valor = round(valor,2) + 0.1

    #se for HE para HE Mariana
    elif inviab.listaTipo[index] == 'HE' and regras.listaTipoRestrFlex[indice] == 'HE':
        
        relato1 = glob.glob('relato.*')
        relato2 = glob.glob('relato2.*')

        RHEEAR_max = 1
        
        # Se for % Mariana
        if inviab.listaLimite[index] == 1: # se for %
            
            valor_inviab = inviab.listaValor[index]
            
            RHEEAR_max = importaRelato.c_relato().leRelatoRHE(relato1[0], inviab.listaEtapa[index], inviab.listaCod[index][0])
            
            valor = 100*(valor_inviab/RHEEAR_max)
            valor = round(valor,1) + 0.1
            
            
            

        # Se for MWmes
        else: # se for MWmes
            
            valor = inviab.listaValor[index]
            valor = round(valor,1) + 1

    #se for vazao para volume
    elif inviab.listaTipo[index] == 'HQ' and regras.listaTipoRestrFlex[indice] == 'HV':

        # passa de m3/s para hm3
        if inviab.listaPatamar[index] == 1:
            valor = inviab.listaValor[index]*(60*60*duracao.pesada)*0.000001
        elif inviab.listaPatamar[index] == 2:
            valor = inviab.listaValor[index]*(60*60*duracao.media)*0.000001
        elif inviab.listaPatamar[index] == 3:
            valor = inviab.listaValor[index]*(60*60*duracao.leve)*0.000001

        valor = round(valor,2) + 0.1

    #se for volume para vazao
    elif inviab.listaTipo[index] == 'HV' and (regras.listaTipoRestrFlex[indice] == 'TI' or regras.listaTipoRestrFlex[indice] == 'HQ'):
        # passa de m3/s para hm3
        valor = inviab.listaValor[index]/((60*60*duracao.total)*0.000001)
        if regras.listaTipoRestrFlex[indice] == 'TI':
            valor = round(valor,2) + 0.1
        elif regras.listaTipoRestrFlex[indice] == 'HQ':
            valor = round(valor,1) + 0.1

    # evaporacao para AC
    elif inviab.listaTipo[index] == 'EV' and regras.listaTipoRestrFlex[indice] == 'AC':
        valor = inviab.listaValor[index]
    # evaporacao para evaporacao
    elif inviab.listaTipo[index] == 'EV' and regras.listaTipoRestrFlex[indice] == 'EV':
        valor = 0
    # Defluencia minima para AC
    elif inviab.listaTipo[index] == 'DM' and regras.listaTipoRestrFlex[indice] == 'DM':

        if inviab.listaPatamar[index] == 1:
            valor = 1 + math.ceil(inviab.listaValor[index]*(duracao.pesada/duracao.total))
        elif inviab.listaPatamar[index] == 2:
            valor = 1 + math.ceil(inviab.listaValor[index]*(duracao.media/duracao.total))
        elif inviab.listaPatamar[index] == 3:
            valor = 1 + math.ceil(inviab.listaValor[index]*(duracao.leve/duracao.total))      

    # Defluencia maxima para porcentagem do VE
    elif inviab.listaTipo[index] == 'HQ' and regras.listaTipoRestrFlex[indice] == 'VE':
        
        # passa de m3/s para hm3
        if inviab.listaPatamar[index] == 1:
            volume_hm3 = inviab.listaValor[index]*(60*60*duracao.pesada)*0.000001
        elif inviab.listaPatamar[index] == 2:
            volume_hm3 = inviab.listaValor[index]*(60*60*duracao.media)*0.000001
        elif inviab.listaPatamar[index] == 3:
            volume_hm3 = inviab.listaValor[index]*(60*60*duracao.leve)*0.000001

        volume_util = usinas_hidr[regras.listaCodRestrFlex[indice]].volUtil
        valor = round(100*volume_hm3/volume_util,2) + 1

    # se as duas forem iguais, retorna o proprio valor
    elif inviab.listaTipo[index] == regras.listaTipoRestrFlex[indice]:
        # Mariana (alterado):
        if inviab.listaTipo[index] == 'TI':
            valor = round(inviab.listaValor[index],2) + 0.2
        elif inviab.listaTipo[index] == 'HQ':
            valor = round(inviab.listaValor[index],1) + 5
        elif inviab.listaTipo[index] == 'RE' or inviab.listaTipo[index] == 'HA' or inviab.listaTipo[index] == 'VE'  or inviab.listaTipo[index] == 'HV':
            valor = round(inviab.listaValor[index],1) + 1
        elif inviab.listaTipo[index] == 'AC':
            valor = inviab.listaValor[index] + 1
        else:
            valor = -1

    else:
        valor = -1

    return valor

#funcao que retira o valor da inviabilizadade no DADGER
# AL - recebeu "num_estagio" a mais
def flexibilizaDADGER(inviab, index, regras, indice, valor, usinas_hidr, iteracao, num_estagio):

    import glob
    import os

    flex = False

    try:
        # obtem o nome do arquivo dadger
        arquivo = glob.glob('dadger.*')
        # obtem o nome do arquivo relato
        arquivo_relato = glob.glob('relato2.*')
        # tenta abrir os arquivos
        with open(arquivo[0], 'r') as arqDADGER, open(arquivo_relato[0], 'r') as arqRELATO2:
            # abre o arquivo que sera escrito o novo DADGER
            with open('dadger_novo.dat', 'w') as arqDADGERnovo:
                #le a linha
                linha = arqDADGER.readline()
                #escreve a linha
                arqDADGERnovo.write(linha)
                # procura a restricao
                # se for restricao Eletrica
                if regras.listaTipoRestrFlex[indice] == 'RE':
                    # AL - recebeu "num_estagio" a mais
                    FlexREHQ(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, 'RE', 'LU', num_estagio)
                # se for restricao de Vazao
                elif regras.listaTipoRestrFlex[indice] == 'HQ':
                    # AL - recebeu "num_estagio" a mais
                    FlexREHQ(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, 'HQ', 'LQ', num_estagio)
                # se for restricao de volume
                elif regras.listaTipoRestrFlex[indice] == 'HV':
                    # AL - recebeu "num_estagio" a mais
                    FlexHVHA(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, 'HV', 'LV', num_estagio)
                # se for restricao de armazenamento minimo Mariana
                elif regras.listaTipoRestrFlex[indice] == 'HE': 
                    # AL - recebeu "num_estagio" a mais
                    FlexHE(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, num_estagio)
                # se for restricao de afluencia
                elif regras.listaTipoRestrFlex[indice] == 'HA':
                    # AL - recebeu "num_estagio" a mais
                    FlexHVHA(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, 'HA', 'LA', num_estagio)
                # se for restricao de irrigacao
                elif regras.listaTipoRestrFlex[indice] == 'TI':
                    # AL - recebeu "num_estagio" a mais
                    FlexTI(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, num_estagio)
                # se for restricao de funcao de producao
                elif regras.listaTipoRestrFlex[indice] == 'FP':
                    FlexFP(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, usinas_hidr, arqRELATO2, int(inviab.listaCenario[index]))
                # se for restricao de vazao minima
                elif regras.listaTipoRestrFlex[indice] == 'DM':
                    FlexDM(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, usinas_hidr)
                # evaporacao para vazao minima
                elif regras.listaTipoRestrFlex[indice] == 'EV' and inviab.listaTipo[index] != 'EV':
                    FlexDM(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, usinas_hidr)
                # se for restricao de evaporacao para tirar a propria evaporacao
                elif regras.listaTipoRestrFlex[indice] == 'EV' and inviab.listaTipo[index] == 'EV':
                    FlexEV(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, usinas_hidr)
                # se for restricao de volume de Espera               
                elif regras.listaTipoRestrFlex[indice] == 'VE':
                    # AL - recebeu "num_estagio" a mais
                    FlexVE(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, num_estagio)
                else:
                    # varre o arquivo
                    while True:
                        linha = arqDADGER.readline()
                        if linha == '':
                            break
                        #escreve a linha
                        arqDADGERnovo.write(linha)

        # deleta o dadger
        os.remove(arquivo[0])
        # renomeia o dadger novo
        os.rename('dadger_novo.dat', arquivo[0])
        # informa que flexibilizou a restricao
        with open('retirainviab.log', 'a') as arqRetInviab:
            if inviab.listaTipo[index] == 'DM' and regras.listaTipoRestrFlex[indice] == 'DM':
                i = int(inviab.listaCod[index][0])
                arqRetInviab.write('Restricao de vazao minima da usina ' + usinas_hidr[i].nome + 'em' + str("%3i" % valor) + ' m3/s\n')
            else:
                arqRetInviab.write(
                    'Restricao ' + inviab.listaTipo[index] + ' ' + str("%3i" % (inviab.listaCod[index][0])) + ' etapa ' +
                    str(inviab.listaEtapa[index]) + ' patamar ' + str(inviab.listaPatamar[index]) +' flexibilizada em ' + str("%.2f" % valor) +
                    ' na restricao ' + regras.listaTipoRestrFlex[indice] + ' ' + str("%3i" % (regras.listaCodRestrFlex[indice])) + '\n')

   # se nao conseguiu eh por que ele nao existe
    except IOError:
        # se nao achou o arquivo, imprime no log e sai da rotina
        print('arquivo dadger.* nao encontrado')
        # escreve no aquivo de Log que nao encontrou o arquivo
        with open('retirainviab.log', 'a') as arqRetInviab:
            arqRetInviab.write(
                'ERRO: arquivo dadger.* nao encontrado, o processo sera interrompido\n')
            flex = False

    return flex

# rotina para retirar flexibilizacao de RE, HQ
# AL - recebeu "num_estagio" a mais
def FlexREHQ(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, MneumoRestr, MneumoLimite, num_estagio):

    # se a regra for para retirar do codigo 0, utiliza o codigo da inviabilidade
    if regras.listaCodRestrFlex[indice] == 0:
        codigoRegra = inviab.listaCod[index][0]
    else:
        codigoRegra = regras.listaCodRestrFlex[indice]

    achou = False
    tresPat = False

    # verifica se a inviabilidade original nao eh para os 3 patamares
    if inviab.listaTipo[index] != 'RE' and inviab.listaTipo[index] != 'HQ':
        tresPat = True

    # varre o arquivo
    while True:
        linha = arqDADGER.readline()
        if not linha:
            break
        if MneumoRestr + '  ' + str("%3i" % codigoRegra) in linha[0:7]:
            achou = True
            # inicializa objeto com as informacoes da restricao
            dados_rest = cDADOS_RESTRICAO(num_estagio)
            # pega os dados de acordo com a regra RE-HQ
            dados_rest.getDadosREHQ(linha, num_estagio)
            # atualiza os valores da restricao
            dados_rest.flexDados(inviab.listaEtapa[index], inviab.listaPatamar[index], tresPat,
                                 regras.listaLimiteFlex[indice], valor)
            break
        # caso contrario, escreve a linha
        arqDADGERnovo.write(linha)
    if achou:
        # escreve a primeira linha (RE ou HQ com seu numero e estagio validos)
        arqDADGERnovo.write(linha)

        # le a proxima e verifica se houve comentario sobre flexibilizacao
        linha = arqDADGER.readline()
        if "Flexibilizado" in linha:
            arqDADGERnovo.write(linha)
        else:
            # Escreve o comentario de flexibilizacao
            arqDADGERnovo.write("& Flexibilizado para convergencia\n")

        # escreve os novos limites (LU)
        # escreve LU primeiro estagio
        if MneumoRestr == "RE":
            mn_aux = "LU"
        else:
            mn_aux = "LQ"
        linha_escrita = str(str(mn_aux).ljust(4) + str(codigoRegra).rjust(3) + str(1).rjust(4) + "   ")
        for ipat in range(3):
            # limite inferior
            linha_escrita += str("%10.1f" % dados_rest.valores_inf[0][ipat])
            # limite superior
            if dados_rest.valores_sup[0][ipat] < 9999999:
                linha_escrita += str("%10.1f" % dados_rest.valores_sup[0][ipat])
            else:
                linha_escrita += str(" ").rjust(10)
        linha_escrita += "\n"
        arqDADGERnovo.write(linha_escrita)
        # escreve demais LUs, se necessario
        for iest in range(1, num_estagio):
            aux_0 = [dados_rest.valores_inf[iest-1], dados_rest.valores_sup[iest-1]]
            aux_1 = [dados_rest.valores_inf[iest], dados_rest.valores_sup[iest]]
            if aux_0 != aux_1:
                linha_escrita = str(str(mn_aux).ljust(4) + str(codigoRegra).rjust(3) + str(iest+1).rjust(4) + "   ")
                for ipat in range(3):
                    # limite inferior
                    linha_escrita += str("%10.1f" % dados_rest.valores_inf[iest][ipat])
                    # limite superior
                    if dados_rest.valores_sup[iest][ipat] < 9999999:
                        linha_escrita += str("%10.1f" % dados_rest.valores_sup[iest][ipat])
                    else:
                        linha_escrita += str(" ").rjust(10)
                linha_escrita += "\n"
                arqDADGERnovo.write(linha_escrita)

        # escreve demais infos
        for ilinha in dados_rest.linhas:
            arqDADGERnovo.write(ilinha)

        # avanca a leitura do arqDADGER ate o ponto ja escrito no arqDADGERnovo
        while linha != dados_rest.linhas[-1]:
            linha = arqDADGER.readline()

        # varre o resto do arquivo arqDADGER e o copia para o arqDADGERnovo
        for linha in arqDADGER:
            # escreve a linha
            arqDADGERnovo.write(linha)
    else:
        # informa que nao achou a restricao
        with open('retirainviab.log', 'a') as arqRetInviab:
            arqRetInviab.write(
                'ERRO: Restricao ' + regras.listaTipoRestrFlex[indice] + ' ' + str("%3i" % (codigoRegra)) + ' etapa ' + str(regras.listaEtapaRestr[indice]) + ' nao encontrada no DADGER\n')

# rotina para retirar flexibilizacao de HV e HA
# AL - recebeu "num_estagio" a mais
def FlexHVHA(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, MneumoRestr, MneumoLimite, num_estagio):

    #se a regra for para retirar do codigo 0, utiliza o codigo da inviabilidade
    if regras.listaCodRestrFlex[indice] == 0:
        codigoRegra = inviab.listaCod[index][0]
    else:
        codigoRegra = regras.listaCodRestrFlex[indice]

    achou = False
    tresPat = True
    comentario = False

    # varre o arquivo
    while True:
        linha = arqDADGER.readline()
        if not linha:
            break
        if MneumoRestr + '  ' + str("%3i" % codigoRegra) in linha[0:7]:
            achou = True
            # inicializa objeto com as informacoes da restricao
            dados_rest = cDADOS_RESTRICAO(num_estagio)
            # pega os dados de acordo com a regra HV-HA
            dados_rest.getDadosHVHA(linha, num_estagio)
            # atualiza os valores da restricao
            dados_rest.flexDados(inviab.listaEtapa[index], inviab.listaPatamar[index], tresPat,
                                 regras.listaLimiteFlex[indice], valor)
            break
        # caso contrario, escreve a linha
        arqDADGERnovo.write(linha)
    if achou:
        # escreve a primeira linha (RE ou HQ, HV HA com seu numero e estagio validos)
        arqDADGERnovo.write(linha)

        # le a proxima e verifica se houve comentario sobre flexibilizacao
        linha = arqDADGER.readline()
        if "Flexibilizado" in linha:
            arqDADGERnovo.write(linha)
        else:
            # Escreve o comentario de flexibilizacao
            arqDADGERnovo.write("& Flexibilizado para convergencia\n")

        # escreve os novos limites (LU)
        # escreve LU primeiro estagio
        if MneumoRestr == "HV":
            mn_aux = "LV"
        else:
            mn_aux = "LA"
        linha_escrita = str(str(mn_aux).ljust(4) + str(codigoRegra).rjust(3) + str(1).rjust(4) + "   ")
        # limite inferior
        linha_escrita += str("%10.2f" % dados_rest.valores_inf[0][0])
        # limite superior
        if dados_rest.valores_sup[0][0] < 9999999:
            linha_escrita += str("%10.2f" % dados_rest.valores_sup[0][0])
        else:
            linha_escrita += str(" ").rjust(10)
        linha_escrita += "\n"
        arqDADGERnovo.write(linha_escrita)
        # escreve demais LUs, se necessario
        for iest in range(1, num_estagio):
            aux_0 = [dados_rest.valores_inf[iest-1][0], dados_rest.valores_sup[iest-1][0]]
            aux_1 = [dados_rest.valores_inf[iest][0], dados_rest.valores_sup[iest][0]]
            if aux_0 != aux_1:
                linha_escrita = str(
                    str(mn_aux).ljust(4) + str(codigoRegra).rjust(3) + str(iest + 1).rjust(4) + "   ")
                # limite inferior
                linha_escrita += str("%10.2f" % dados_rest.valores_inf[iest][0])
                # limite superior
                if dados_rest.valores_sup[iest][0] < 9999999:
                    linha_escrita += str("%10.2f" % dados_rest.valores_sup[iest][0])
                else:
                    linha_escrita += str(" ").rjust(10)
                linha_escrita += "\n"
                arqDADGERnovo.write(linha_escrita)

        # escreve demais infos
        for ilinha in dados_rest.linhas:
            arqDADGERnovo.write(ilinha)

        # avanca a leitura do arqDADGER ate o ponto ja escrito no arqDADGERnovo
        while linha != dados_rest.linhas[-1]:
            linha = arqDADGER.readline()

        # varre o resto do arquivo arqDADGER e o copia para o arqDADGERnovo
        for linha in arqDADGER:
            # escreve a linha
            arqDADGERnovo.write(linha)
    else:
        # informa que nao achou a restricao
        with open('retirainviab.log', 'a') as arqRetInviab:
            arqRetInviab.write(
                'ERRO: Restricao ' + regras.listaTipoRestrFlex[indice] + ' ' + str("%3i" % (codigoRegra)) + ' etapa ' + str(regras.listaEtapaRestr[indice]) + ' nao encontrada no DADGER\n')

# Mariana
def FlexHE(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, num_estagio):

    #se a regra for para retirar do codigo 0, utiliza o codigo da inviabilidade
    if regras.listaCodRestrFlex[indice] == 0:
        codigoRegra = inviab.listaCod[index][0]
    else:
        codigoRegra = regras.listaCodRestrFlex[indice]

    achou = False

    # obtem a etapa
    etapa_flex = inviab.listaEtapa[index]
        
    # varre o arquivo
    
    for linha in arqDADGER:

        if (('HE  ' + str("%3i" % codigoRegra) in linha[0:10]) and (str(etapa_flex) in linha[25:27])):
            #if int(linha[25:26].strip()) == etapa_flex:
            achou = True
            break
        else:
            # escreve a linha
            arqDADGERnovo.write(linha)
            if 'Flexibilizado' in linha:
                comentarioFlex = True
            else:
                comentarioFlex = False

    if achou:
        #escreve o comentario
        if not comentarioFlex:
            arqDADGERnovo.write('& Flexibilizado para convergencia\n')

        flag_penaliza = linha[43:44]
        flag_penaliza = flag_penaliza.strip()
        
        #if flag_penaliza != '1':
        
        # comeca a escrever a nova linha ate antes do valor
        novaLinha = linha[0:13]
        
        novoValor = (float(linha[14:23]) - valor)
        if novoValor <= 0:
            novoValor = 0
        
        # flexibiliza a restricao naquele estagio
        novaLinha += "%10.1f" % (novoValor)
        
        # escreve o resto da linha
        
        novaLinha += linha[23:43] 
        #else:
        #novaLinha = linha[0:43]
        if novaLinha[-1] != '\n':
        
            novaLinha += '\n'

        # escreve nova linha
        arqDADGERnovo.write(novaLinha)

        # varre o arquivo Mariana
        for linha in arqDADGER:
            # escreve a linha
            arqDADGERnovo.write(linha)
        
    else:
        # informa que nao achou a restricao
        with open('retirainviab.log', 'a') as arqRetInviab:
            arqRetInviab.write(
                'ERRO: Restricao ' + regras.listaTipoRestrFlex[indice] + ' ' + str("%3i" % codigoRegra) + ' etapa ' + str(regras.listaEtapaRestr[indice]) + ' nao encontrada no DADGER\n')

            
# rotina para retirar flexibilizacao de TI
# AL - recebeu "num_estagio" a mais
def FlexTI(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, num_estagio):

    #se a regra for para retirar do codigo 0, utiliza o codigo da inviabilidade
    if regras.listaCodRestrFlex[indice] == 0:
        codigoRegra = inviab.listaCod[index][0]
    else:
        codigoRegra = regras.listaCodRestrFlex[indice]

    achou = False

    # varre o arquivo
    for linha in arqDADGER:

        if 'TI  ' + str("%3i" % codigoRegra) in linha[0:7]:
            achou = True
            break
        else:
            # escreve a linha
            arqDADGERnovo.write(linha)
            if 'Flexibilizado' in linha:
                comentarioFlex = True
            else:
                comentarioFlex = False

    if achou:
        #escreve o comentario
        if not comentarioFlex:
            arqDADGERnovo.write('& Flexibilizado para convergencia\n')

        # obtem a etapa
        etapa_flex = inviab.listaEtapa[index]
        # comeca a escrever a nova linha ate antes da etapa
        novaLinha = linha[0:9+5*(etapa_flex-1)]
        # flexibiliza a restricao naquele estagio
        if float(linha[9+5*(etapa_flex-1):14+5*(etapa_flex-1)]) - valor >= 0:
            novaLinha += "%5.2f" % (float(linha[9+5*(etapa_flex-1):14+5*(etapa_flex-1)]) - valor)
        else:
            novaLinha += " 0.00"
        # escreve o resto da linha
        novaLinha += linha[14+5*(etapa_flex-1):-1] + '\n'

        # escreve nova linha
        arqDADGERnovo.write(novaLinha)

        # varre o arquivo
        for linha in arqDADGER:
            # escreve a linha
            arqDADGERnovo.write(linha)
    else:
        # informa que nao achou a restricao
        with open('retirainviab.log', 'a') as arqRetInviab:
            arqRetInviab.write(
                'ERRO: Restricao ' + regras.listaTipoRestrFlex[indice] + ' ' + str("%3i" % codigoRegra) + ' etapa ' + str(regras.listaEtapaRestr[indice]) + ' nao encontrada no DADGER\n')

# rotina para retirar flexibilizacao de funcao de producao
def FlexFP(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, usinas_hidr, arqRELATO2, cenario):

    #se a regra for para retirar do codigo 0, utiliza o codigo da inviabilidade
    if regras.listaCodRestrFlex[indice] == 0:
        codigoRegra = inviab.listaCod[index][0]
    else:
        codigoRegra = regras.listaCodRestrFlex[indice]

    achou1 = False

    # varre o arquivo dadger
    while True:
        linha = arqDADGER.readline()
        if linha == '':
            break
        
        if '&   usi  iper tp Npt  QMin  QMax  tp Npt  VMin  VMax  GHmin GHmax Tol FlgD  tp %/n    %/n NI Verif' in linha:
            achou1 = True
            break
        else:
            arqDADGERnovo.write(linha)    

    # achou1 é para flexibilizar a FPH dos dois estágios
    if achou1:
       
        arqDADGERnovo.write(linha)

        #pula e escreve a regua
        linha = arqDADGER.readline()
        arqDADGERnovo.write(linha)

        #escreve o comentario
        arqDADGERnovo.write('& Flexibilizado para convergencia - ' + str(inviab.listaCod[index][1]) + '\n')

        # escreve a linha
        novaLinha = 'FP  ' + str("%3i" % codigoRegra) + '    1  0   20     0   100  0   20 ' + str("%5i" % (100)) + ' ' + str("%5i" % (100)) + '\n'
        arqDADGERnovo.write(novaLinha)

    # varre o arquivo
    while True:
        linha = arqDADGER.readline()

        # apaga se ha uma linha previamente escrita sobre esta restrição
        if str('& Flexibilizado para convergencia - ' + str(inviab.listaCod[index][1])) in linha:
            linha = arqDADGER.readline()

        # apaga se ha uma linha previamente escrita sobre esta restrição
        if 'FP  ' + str("%3i" % codigoRegra) + '    1' in linha:
            linha = arqDADGER.readline()

        if linha == '':
            break
        # le a 2 linha
        arqDADGERnovo.write(linha)

# rotina para retirar flexibilizacao de vazao minima
def FlexDM(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, usinas_hidr):

    #se a regra for para retirar do codigo 0, utiliza o codigo da inviabilidade
    if regras.listaCodRestrFlex[indice] == 0:
        codigoRegra = inviab.listaCod[index][0]
    else:
        codigoRegra = regras.listaCodRestrFlex[indice]

    achou1 = False
    achou2 = False

    # varre o arquivo
    while True:
        linha = arqDADGER.readline()
        if linha == '':
            break
        if 'AC' == linha[0:2] and 'VAZMIN' == linha[9:15] and linha[4:7] == str("%3i" % codigoRegra):
            achou1 = True
            break
        else:
            arqDADGERnovo.write(linha)    

    if achou1:

        valor_antigo = int(linha[16:24])

        # previne que o valor fique negativo
        if (valor_antigo - valor) < 0:
            valor_a_ser_usado = 0
        else:
            valor_a_ser_usado = valor_antigo - valor

        # escreve a linha
        novaLinha = 'AC  ' + str("%3i" % codigoRegra) + '  VAZMIN    ' + str("%5i" % (valor_a_ser_usado)) + '\n'
        arqDADGERnovo.write(novaLinha)
    
    else:

        arqDADGER.seek(0)
        arqDADGERnovo.seek(0)
        
        while True:
            linha = arqDADGER.readline()
            if linha == '':
                break
            if 'AC' == linha[0:2] and 'VAZMIN' == linha[9:15]:
                achou2 = True
                break
            else:
                arqDADGERnovo.write(linha)                  

    if achou2:

        arqDADGERnovo.write(linha)
        #escreve o comentario
        arqDADGERnovo.write('& Flexibilizado para convergencia - ' + str(inviab.listaCod[index][1]) + '\n')

        i = int(inviab.listaCod[index][0])
        vazao_minima = round(usinas_hidr[i].vazaoMin,0) 

        # escreve a linha
        novaLinha = 'AC  ' + str("%3i" % codigoRegra) + '  VAZMIN    ' + str("%5i" % (vazao_minima - valor)) + '\n'            
        arqDADGERnovo.write(novaLinha)

    # varre o arquivo
    while True:
        linha = arqDADGER.readline()
        if linha == '':
            break
        # le a 2 linha
        arqDADGERnovo.write(linha)

def FlexEV(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, usinas_hidr):

    #se a regra for para retirar do codigo 0, utiliza o codigo da usina
    if regras.listaCodRestrFlex[indice] == 0:
        codigoRegra = inviab.listaCod[index][0]
    else:
        codigoRegra = regras.listaCodRestrFlex[indice]

    # varre o arquivo
    while True:
        linha = arqDADGER.readline()
        if linha == '':
            break
        if 'UH' == linha[0:2] and linha[4:7] == str("%3i" % codigoRegra):
            novaLinha = linha[0:39] + '0                              ' + '\n'
            arqDADGERnovo.write(novaLinha)
            break
        else:
            arqDADGERnovo.write(linha)

    # varre o arquivo
    while True:
        linha = arqDADGER.readline()
        if linha == '':
            break
        # le a 2 linha
        arqDADGERnovo.write(linha)

# rotina para retirar flexibilizacao de VE
# AL - recebeu "num_estagio" a mais
def FlexVE(arqDADGER, arqDADGERnovo, inviab, index, regras, indice, valor, num_estagio):


    achou = False

    #se a regra for para retirar do codigo 0, utiliza o codigo da inviabilidade
    if regras.listaCodRestrFlex[indice] == 0:
        codigoRegra = inviab.listaCod[index][0]
    else:
        codigoRegra = regras.listaCodRestrFlex[indice]

    # varre o arquivo
    for linha in arqDADGER:

        if 'VE  ' + str("%3i" % codigoRegra) in linha[0:7]:
            achou = True
            break
        else:
            # escreve a linha
            arqDADGERnovo.write(linha)
            if 'Flexibilizado' in linha:
                comentarioFlex = True
            else:
                comentarioFlex = False

    if achou:
        #escreve o comentario
        if not comentarioFlex:
            arqDADGERnovo.write('& Flexibilizado para convergencia\n')

        # obtem a etapa
        etapa_flex = inviab.listaEtapa[index]

        # comeca a escrever a nova linha ate antes da etapa
        novaLinha = linha[0:9 + 5 * (etapa_flex - 1)]

        # flexibiliza a restricao naquele estagio
        if float(linha[9 + 5 * (etapa_flex - 1):14 + 5 * (etapa_flex - 1)]) + valor <= 100:
            novaLinha += "%5.2f" % (float(linha[9 + 5 * (etapa_flex - 1):14 + 5 * (etapa_flex - 1)]) + valor)
        else:
            novaLinha += "100.0"

        # escreve o resto da linha
        novaLinha += linha[14 + 5 * (etapa_flex - 1):-1] + '\n'

        # escreve nova linha
        arqDADGERnovo.write(novaLinha)

        # varre o arquivo
        for linha in arqDADGER:
            # escreve a linha
            arqDADGERnovo.write(linha)

    else:
        # informa que nao achou a restricao
        with open('retirainviab.log', 'a') as arqRetInviab:
            arqRetInviab.write(
                'ERRO: Restricao ' + regras.listaTipoRestrFlex[indice] + ' ' + str("%3i" % codigoRegra) + ' etapa ' + str(regras.listaEtapaRestr[indice]) + ' nao encontrada no DADGER\n')

# AL - funcao para obter numero de estagios do caso
def getNumEstagio():
    estagio = 0
    try:
        arquivo = glob.glob('dadger.*')
        with open(arquivo[0], 'r') as arqDADGER1:
            for linha1 in arqDADGER1:
                if "DP  " in linha1[0:4]:
                    estagio = int(linha1[4:6].strip())
                if linha1 == '':
                    break
            return estagio
    except IOError:
        print('Arquivo DADGER.* nao encontrado')
        return estagio