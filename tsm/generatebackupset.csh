#! /bin/csh -f

# crontab:
# 0 8 * * 5 /tsm/bin/generatebackupset.csh > /tmp/generatebackupset.log 2>&1

set date=20`date +%y%m%d`

date
echo /tsm/bin/dsmadmc0 GENERATE BACKUPSET clone clone${date} devclass=ltodvc toc=no

/tsm/bin/dsmadmc0 GENERATE BACKUPSET clone clone${date} devclass=ltodvc toc=no

