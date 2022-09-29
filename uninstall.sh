#! /bin/bash

USERINSTALLDIR=/home/pem/rotinas
INSTALLDIR=${USERINSTALLDIR}/encadeador-pem
echo "Removendo arquivos da instalação em ${INSTALLDIR}" 
[ -d $INSTALLDIR ] && rm -r $INSTALLDIR

EXECPATH=/usr/bin/encadeador-newave-decomp
echo "Removendo executável em ${EXECPATH}" 
[ -f $EXECPATH ] && rm $EXECPATH
echo "Finalizando..."
