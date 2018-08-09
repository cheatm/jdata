#! /bin/bash

python export.py > /etc/profile.d/env.sh
crontab /app/routing/schedule
/usr/sbin/cron -f