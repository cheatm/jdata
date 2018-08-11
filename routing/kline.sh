#! /bin/bash
source /etc/profile

if [ $WORK ]
then
    cd $WORK
else
    cd /app
fi

if [ $1 == index ]
then
    python jaqsd/kline/index.py
fi

if [ $1 == daily ]
then
    python jaqsd/kline/daily.py download
fi

if [ $1 == bar ]
then
    python jaqsd/kline/bar.py download
fi
    