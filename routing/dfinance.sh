#! /bin/bash
source /etc/profile

if [ $WORK ]
then
    cd $WORK
else
    cd /app
fi

python jaqsd/finance/daily.py $*

