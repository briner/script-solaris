#!/bin/csh -f

# crontab:
# 33 3 * * * /tsm/bin/backup.csh > /tmp/tsmbackup.log 2>&1

/usr/local/bin/backupservices before
dsmc incr /
dsmc incr /var
dsmc incr /oracle_bk
dsmc incr /home -snapshotroot=/home/.backup
dsmc incr /oracle_bt -snapshotroot=/oracle_bt/.backup
dsmc incr /oracle_dev -snapshotroot=/oracle_dev/.backup
dsmc incr /oracle_prod -snapshotroot=/oracle_prod/.backup
dsmc incr /unige -snapshotroot=/unige/.backup
/usr/local/bin/backupservices after
