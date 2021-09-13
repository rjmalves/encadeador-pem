# -*- coding: latin-1 -*-

import numpy as np
import sys
import glob

class c_relato:

    def __init__(self):

        Qtur_usina = []
        geracao_media = []
        produtibilidade = []
        RHE_EARmax = 0

        num_cenarios = 0

    def leRelatoRHE(self, relato_entrada, etapa, codigo):

        dadger = glob.glob('dadger.*')
        # Importa energia armazenavel maxima  REE
        try:
            with open(relato_entrada, 'r') as relato:
                linhas = relato.readlines()
        except:
            sys.exit('Relato nao encontrado')
        with open(relato_entrada, 'r') as relato:
            RHE_EARmax = 0
            while True:
                line = relato.readline()
                
                if line=='':
                    break
                #print("Aqui 1: antes do Relatorio dos Dados das Restricoes Especiais de Energia Armazenada   (RHE)") Mariana
                if 'Relatorio dos Dados das Restricoes Especiais de Energia Armazenada   (RHE)' in line:
                    while True:
                        line = relato.readline()
                        if line=='':
                            break
                        #print("Aqui 2: antes do Restricoes para o periodo") Mariana
                        if str('Restricoes para o periodo'  + str(etapa).rjust(3)) in line:
                            
                            while True:
                                line = relato.readline()
                                if line=='':
                                    break
                                #print("Aqui 3: antes do RHE  ") Mariana
                                #print(str('RHE' + str(codigo).rjust(4))) Mariana
                                #print(codigo) Mariana
                                #print codigo Mariana
                                #print(str(codigo).rjust(4)) Mariana
                                if str('RHE' + str(codigo).rjust(4)) in line:
                                    #print("Aqui 4: Entrou no if do RHE") Mariana
                                    RHE_EARmax = float(line[21:32])/(float(line[40:46])/100)
                                    break
                    #while not str('Restricoes para o periodo'  + str(etapa).rjust(3)) in line: Mariana
                    #    line = relato.readline()
                    #    if line=='':
                    #        break
                    #    if str('RHE' + str(codigo).rjust(4)) in line:
                    #        RHE_EARmax = float(line[21:32])/float(line[40:46])
                    #
        return RHE_EARmax

    
    def leRelato(self, relato_entrada, usina):

        dadger = glob.glob('dadger.*')

        # Importa vazao turbinavel

        try:

            with open(relato_entrada, 'r') as relato:

                linhas = relato.readlines()

        except:

            sys.exit('Relato nao encontrado')

        with open(relato_entrada, 'r') as relato:

            num1 = 0
            num2 = 0

            for line in relato:

                if 'RELATORIO  DO  BALANCO  HIDRAULICO' in line:

                    linha_inicial = num1 + 8

                elif 'Relatorio das Restricoes Hidraulicas  de Vazao Afluente (m3/s)' in line:

                    linha_final = num2 - 2

                num1 += 1
                num2 += 1

            pass        
            
        with open(relato_entrada, 'r') as relato:

            linhas = relato.readlines()

            for i in range(linha_inicial, linha_final):

                linha = linhas[i]

                if linha[4:16].strip() == usina.strip():

                    Qtur_usina = (float(linha[37:43].strip()))

        # Importa geracao termica

        with open(relato_entrada, 'r') as relato:

            num1 = 0
            num2 = 0

            for line in relato:

                if 'Ini.  Fin.  Esp.   Qnat   (  %MLT)   Qafl     Qdef    GER_1   GER_2   GER_3    Media   VT(*)   VNT    Ponta   FPCGC' in line:

                    linha_inicial = num1 + 2

                elif '(*) OBS.: os valores da energia vertida turbinavel contem os desvios da funcao de producao' in line and i == 1:

                    linha_final = num2 - 11

                num1 += 1
                num2 += 1

            pass

        with open(relato_entrada, 'r') as relato:

            linhas = relato.readlines()

            for i in range(linha_inicial, linha_final):

                linha = linhas[i]

                if linha[9:21].strip() == usina.strip():

                    geracao_media = (float(linha[105:111].strip()))

                    produtibilidade = (geracao_media/Qtur_usina)

        return produtibilidade
