#! /bin/bash

python export.py > /etc/profile.d/env.sh
crontab $WORK/routing/schedule
/usr/sbin/cron -f