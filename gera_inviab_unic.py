# -*- coding: latin-1 -*-
# Rotina para transformar o arquivo inviabi_unic.txt no formato inviab_unic.rvx

import glob
import os
import sys

rev = sys.argv[1]
#rev = rev[-1:]

#monta o caminho do arquivo
inviab_txt = glob.glob("inviabi_unic.txt")
filename = "inviab_unic." + rev

# dadger = glob.glob(os.path.join("..", "..", self.dirDestino, "DADGER.*"))
try:
    # tenta abrir o arquivo
    with open(inviab_txt[0], 'r') as arq_txt:

        # abre o arquivo que sera escrito o novo DADGER
        with open(filename, 'w') as arq_inviab_unic:
            # escreve cabecalhos do inviab_unic
            arq_inviab_unic.write("Arquivo inviab_unic." + rev + " gerado pela rotina python\n")
            arq_inviab_unic.write("   SIMULACAO FINAL:                                                                                                                                   \n")
            arq_inviab_unic.write("   X--------X--------X----------------------------------------------------------------------------X----------------------X                            \n")
            arq_inviab_unic.write("    ESTAGIO  CENARIO         RESTRICAO VIOLADA                                                           VIOLACAO                                     \n")
            arq_inviab_unic.write("   X--------X--------X----------------------------------------------------------------------------X----------------------X                            \n")
     
            # varre o arquivo inviabi_txt
            while True:
                linha = arq_txt.readline()
                if not linha: break
                
                novalinha = linha[22:33] + linha[35:45] + "  " + linha[47:100] + "                        " + linha[100:]
                arq_inviab_unic.write(novalinha)
                
    # deleta o dadger
    #os.remove(inviab_txt[0])
    # renomeia o dadger novo
    #os.rename(os.path.join("..", "..",  self.dirDestino, "dadger_novo.dat"), dadger[0])

except IOError:
    # informa que nao achou o arquivo
    erro = True

