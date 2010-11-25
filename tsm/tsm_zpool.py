#!/usr/bin/python

print 'we launch tsm_pool'

import os, sys
from optparse import OptionParser
from datetime import datetime
import subprocess as s
import logging
import signal
import select


SNAPNAME='tsmzpool.'+str(os.getpid())
SNAPNAME='tsmzpool'
TMPDIR='/tmp/tsm_zpool'
VERBOSE=False
DRYRUN=False


ZFS_LIST_CMD="zfs list -H -o name -t filesystem"
ZFSALLSNAP_CMD="/usr/local/bin/zfsallsnap snapshot --backup --clobber %(zonename)s@%(snapname)s"
ZFSREMOVEALLSNAP_CMD="/usr/local/bin/zfsallsnap destroy %(zonename)s@%(snapname)s"
MOUNT_CMD='/usr/sbin/mount'
DSMC_CMD='dsmc incr -servername=%(servername)s %(zfsdir)s -snapshotroot=%(snapdir)s'

#
# logging stuff
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='/var/log/tsm_zpool',
#                    filename=sys.stdout,
                    filemode='w')

# management of SIGTERM
#class CallbackSignal(object):
#    def __init__(self):
#        self.subrocess_id=None    
#    def termination(signum, frame):
#        if self.subproces_id:
#            os.kill(subprocess_id, signal.sigterm)
#        
#callback_signal=CallbackSignal()
#signal.signal(signal.SIGTERM, callback_signal.termination)

#
# here we go
#
def check_if_zfs(zfsname):
    proc=s.Popen(ZFS_LIST_CMD, stdout=s.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        logging.error('the cmd (%s) did not succeed' % ZFS_LIST_CMD)
    lout=[out.rstrip() for out in lout]
    ret=zfsname in lout
    return ret

def zfsallsnap(zonename):
    inst_cmd=ZFSALLSNAP_CMD % {'zonename': zonename, 'snapname': SNAPNAME }
    logging.debug('zfsallsnap cmd:'+inst_cmd)
    proc=s.Popen(inst_cmd, stdout=s.PIPE, stderr=s.PIPE, shell=True, cwd='/')
    stdout, stderr = proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        logging.error('the cmd (%s) did not succeed' % inst_cmd)
        raise Exception( 'zfsallsnap problem')

def zfsremoveallsnap(zonename):
    inst_cmd=ZFSREMOVEALLSNAP_CMD % {'zonename': zonename, 'snapname': SNAPNAME }
    logging.debug('zfsremoveallsnap cmd:'+inst_cmd)
    proc=s.Popen(inst_cmd, stdout=s.PIPE, stderr=s.PIPE, shell=True, cwd='/')
    stdout, stderr = proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        logging.error('the cmd (%s) did not succeed' % inst_cmd)
        raise Exception( 'zfsremoveallsnap problem')

def get_dfs_mountpoint():
    proc=s.Popen(MOUNT_CMD, stdout=s.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        logging.error('the cmd (%s) did not succeed' % s)
    lout=[out.rstrip().split(' ') for out in lout]
    dfs_mountpoint={}
    for out in lout:
        dfs_mountpoint[out[2]]=out[0]
    return dfs_mountpoint

def construct_lzfs_to_backup(zpoolname):
    proc=s.Popen(ZFS_LIST_CMD, stdout=s.PIPE, shell=True, cwd='/')
    lzfs=proc.stdout.readlines()
    lzfs=[zfs.rstrip() for zfs in lzfs]
    retcode=proc.wait()
    if retcode != 0 :
        logging.error('the cmd (%s) did not succeed' % ZFS_LIST_CMD)
    dfs_mountpoint=get_dfs_mountpoint()
    lzfsdir_snapdir=[]
    for zfs in lzfs:
        if zfs.find(zpoolname) != 0:
            continue
        zfs_dirpath=dfs_mountpoint.get(zfs)
        if not zfs_dirpath:
            logging.error("zfs (%s) doesn't have a mountpoint" % zfs_dirpath)
            continue
        #check that the mountpoint of the snapshot is there
        snapshot_dirpath=zfs_dirpath+'/.zfs/snapshot/'+SNAPNAME
        try:
            os.stat(snapshot_dirpath)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            logging.error("zfs (%s) doesn't have a mountpoint (%s) for the snapshot" % (zfs, snapshot_dirpath))
            continue
        lzfsdir_snapdir.append([zfs_dirpath, snapshot_dirpath])
    return lzfsdir_snapdir

def backup_directory(zfsdir, snapdir, tsm_servername):
    logging.info('backup zfs (%s)' % zfsdir)
    #DSMC_CMD='dsmc incr -servername=%(servername)s %(zfsdir)s -snapshotroot=%(snapdir)s'
    inst_cmd=DSMC_CMD % { 'zfsdir' : zfsdir
                        , 'snapdir' : snapdir
                        , 'servername' : tsm_servername } # value specified in dsm.sys }
    #inst_cmd='/usr/local/bin/qq.bash'
    logging.debug('backup zfs (%s) with cmd (%s)' % (zfsdir, inst_cmd) )
    proc=s.Popen(inst_cmd, stdout=s.PIPE, stderr=s.PIPE, shell=True, cwd='/')
    read_set=[proc.stdout, proc.stderr]
    write_set=x_set=[]
    while read_set:
        rlist,wlist,xlist=select.select(read_set, [], [])
        if proc.stdout in rlist:
            stdout=proc.stdout.readline()
            if stdout == '':
                read_set.remove(proc.stdout)
            else:
                logging.info("dsmc (out): %s" % stdout.rstrip())
        if proc.stderr in rlist:
            stderr=proc.stderr.readline()
            if stderr == '':
                read_set.remove(proc.stderr)
            else:
                logging.info("dsmc (err): %s" % stderr.rstrip())   
    


def backup_zpool(zpoolname, tsm_servername):
    #
    # is a zfs FS
    logging.info('backup zpool (%s)' % zpoolname)
    logging.info('check it zpool (%s) is a zfs FS' % zpoolname)
    if not check_if_zfs(zpoolname):
        logging.error('can not backup zpool (%s), because it is not a zpool' % zpoolname)
        raise Exception('can not backup zpool (%s), because it is not a zpool' % zpoolname)
    #
    # snapshot zpool
    logging.info('snapshot (%s), and its childs pool' % zpoolname)
    zfsallsnap(zpoolname)
    #
    # construct the list of zfs FS to backup
    logging.info('construct list of zfs to backup')
    lzfsdir_snapdir=construct_lzfs_to_backup(zpoolname)
    #
    # backup it
    for zfsdir_snapdir in lzfsdir_snapdir:
        logging.info('backup dir %s' % zfsdir_snapdir[0])
        backup_directory(zfsdir_snapdir[0], zfsdir_snapdir[1], tsm_servername)
    #
    # remove snapshot zpool
    logging.info('remove snapshot (%s), and its childs pool' % zpoolname)
    zfsremoveallsnap(zpoolname)
    logging.info('zpool (%s) backuped' % zpoolname)


if '__main__' == __name__:
    parser = OptionParser(usage="%prog [option] zpool_name ...")
    parser.add_option("-v", "--file", action="store_true", dest="isverbose", default=False)
    parser.add_option("-n", "--dryrun", action="store_true", dest="isdryrun", default=False)
    (options, args) = parser.parse_args()
    if options.isverbose:
        VERBOSE=True
    if options.isdryrun:
        DRYRUN=True
    if len(args) != 1:
        parser.print_help()
        sys.exit(1)
    zpoolname=args[0]
    backup_zpool(zpoolname, zpoolname)
    sys.exit()
