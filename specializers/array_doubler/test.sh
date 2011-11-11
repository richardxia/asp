#!/bin/bash

PYTHONPATH=../../:$PYTHONPATH
CLASSPATH=../../avroInter:$CLASSPATH

echo PYTHONPATH
echo ${PYTHONPATH}

if [ -z "${PYTHON}" ]
then
    PYTHON=python
fi
if [ -z "${PYTHONARGS}" ]
then
    PYTHONARGS=
fi

PYTHONPATH=`pwd`:${PYTHONPATH} CLASSPATH=${CLASSPATH} ${PYTHON} ${PYTHONARGS} test.py
