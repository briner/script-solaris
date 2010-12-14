#!/usr/bin/python
# add management of the lock
# add management of the interruption
# add logging systems
# TODO: put a file accessible in the zone and in the master which gives info
#       to be able to backup also within the zone
# TODO: check saveset, and look how it manages the backup, by adding info such as
#       "zfs get RECURSIF all <zpool> > /zones/zpool/zpool_get_all.info" bakcup it
#       and remove it at the end of the backup. Attention to remove it also when the
#       the signal 15 arrives 
# TODO: look what have be done on geneva2003 and africa-home
# TODO: send email to the destination written in the alias of the zone



import os, sys, shutil
from optparse import OptionParser
from datetime import datetime
import subprocess as s
import logging
import signal
import select


SNAPNAME='tsmzpool.'+str(os.getpid())
TMPDIR='/tmp/tsm_zpool'
VERBOSE=False
DRYRUN=False
VAR_DIR='/var/run'
LOG_FILENAME='/var/log/tsm_zpool'


ZFS_LIST_CMD="zfs list -H -o name -t filesystem"
ZFSALLSNAP_CMD="/usr/local/bin/zfsallsnap snapshot --backup --clobber %(zonename)s@%(snapname)s"
ZFSREMOVEALLSNAP_CMD="/usr/local/bin/zfsallsnap destroy %(zonename)s@%(snapname)s"
MOUNT_CMD='/usr/sbin/mount'
DSMC_BACKUP='dsmc incr -servername=%(servername)s %(zfsdir)s -snapshotroot=%(snapdir)s'
DSMC_RESTORE='dsmc restore -servername=tstore1_pool -subdir=yes -preservepath=subtree /zones/tstore1/var/ /root_pool/restore_tstore1/var/'

#
# logging stuff
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

my_logger = logging.getLogger('MyLogger')
my_logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(LOG_FILENAME)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s") )
my_logger.addHandler(file_handler)

def to_stdout():
    stream_handler = logging.StreamHandler(sys.stdout)
    my_logger.addHandler(stream_handler)

def verbose():
#    TODO:
    pass
 

class CallbackSignal(object):
    def __init__(self):
        self.ltask=[]
        self.index=0
    def add_task(self, task, taskname=None):
        if not taskname:
            taskname=self.index
            self.index+=1
        my_logger.debug('callback_signal add task (%s)' % taskname)
        self.ltask.append((taskname, task))
        return taskname
    def del_task(self, taskname):
        found=True
        for task, task_name in self.ltask:
            if task_name == taskname:
                break
        else:
            found=False
        if found:
            self.ltask.remove(taskname,task)
    def termination(self, signum, frame):
        my_logger.debug('CTRL-C catched')
        for taskname, task in reversed (self.ltask):
            my_logger.info('launch task (%s)' % taskname)     
            task()        
callback_signal=CallbackSignal()
signal.signal(signal.SIGINT, callback_signal.termination)
signal.signal(signal.SIGTERM, callback_signal.termination)


#
# here we go
#
class Lock(object):
    def remove(self):
        os.remove(self.fn_lock)
        my_logger.debug('unlocked')
    def write(self):        
        callback_signal.add_task(self.remove, 'Lock.remove')
        fh_lock=file(self.fn_lock, 'w')
        fh_lock.write(str(os.getpid()))
        fh_lock.close()
        my_logger.debug('locked')
    def it(self, zfsname):
        self.fn_lock=os.path.join(VAR_DIR, "tsm_zpool."+zfsname)
        my_logger.debug('enter in "lock_it"')
        #
        # /var/run/tsm_zpool
        if not os.path.isdir(VAR_DIR):
            os.mkdir(VAR_DIR)
        if not os.path.isfile( self.fn_lock ):
            my_logger.debug('take the lock, the easy way')
            # hiha.., no lock file :)
            self.write()
            return
        #
        # Ouch, we got a lock 
        fh_lock=file(self.fn_lock, 'r')
        pid=int(fh_lock.read())
        is_process=True
        try:
            os.kill(pid,0)
        except:
            is_process=False
        if is_process:
            my_logger.info('can not take the lock, an other instance with pid (%s) is already running' % pid)
            print 'can not take the lock, an other instance with pid (%s) is already running' % pid
            sys.exit(1)
        else:
            my_logger.debug('take the lock, the hard way (a lock file, with a pid, without the relative process)')
            self.write()
lock=Lock()
    
def check_if_zfs(zfsname):
    my_logger.debug('enter in "check_if_zfs"')
    proc=s.Popen(ZFS_LIST_CMD, stdout=s.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % ZFS_LIST_CMD)
    lout=[out.rstrip() for out in lout]
    ret=zfsname in lout
    return ret
    
def zfsallsnap(zonename):
    my_logger.debug('enter in "zfsallsnap"')
    inst_cmd=ZFSALLSNAP_CMD % {'zonename': zonename, 'snapname': SNAPNAME }
    my_logger.debug('zfsallsnap cmd:'+inst_cmd)
    proc=s.Popen(inst_cmd, stdout=s.PIPE, stderr=s.PIPE, shell=True, cwd='/')
    stdout, stderr = proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        my_logger.error('the cmd (%s) did not succeed' % inst_cmd)
        raise Exception( 'zfsallsnap problem')

def zfsremoveallsnap(zonename):
    inst_cmd=ZFSREMOVEALLSNAP_CMD % {'zonename': zonename, 'snapname': SNAPNAME }
    my_logger.debug('zfsremoveallsnap cmd:'+inst_cmd)
    proc=s.Popen(inst_cmd, stdout=s.PIPE, stderr=s.PIPE, shell=True, cwd='/')
    stdout, stderr = proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        my_logger.error('the cmd (%s) did not succeed' % inst_cmd)
        raise Exception( 'zfsremoveallsnap problem')

def get_dfs_mountpoint():
    proc=s.Popen(MOUNT_CMD, stdout=s.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % s)
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
        my_logger.error('the cmd (%s) did not succeed' % ZFS_LIST_CMD)
    dfs_mountpoint=get_dfs_mountpoint()
    lzfsdir_snapdir=[]
    for zfs in lzfs:
        if zfs.find(zpoolname) != 0:
            continue
        zfs_dirpath=dfs_mountpoint.get(zfs)
        if not zfs_dirpath:
            my_logger.error("zfs (%s) doesn't have a mountpoint" % zfs_dirpath)
            continue
        #check that the mountpoint of the snapshot is there
        snapshot_dirpath=zfs_dirpath+'/.zfs/snapshot/'+SNAPNAME
        try:
            os.stat(snapshot_dirpath)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            my_logger.error("zfs (%s) doesn't have a mountpoint (%s) for the snapshot" % (zfs, snapshot_dirpath))
            continue
        lzfsdir_snapdir.append([zfs_dirpath, snapshot_dirpath])
    return lzfsdir_snapdir

def backup_directory(zfsdir, snapdir, tsm_servername):
    my_logger.info('backup zfs (%s)' % zfsdir)
    #DSMC_BACKUP='dsmc incr -servername=%(servername)s %(zfsdir)s -snapshotroot=%(snapdir)s'
    inst_cmd=DSMC_BACKUP % { 'zfsdir' : zfsdir
                        , 'snapdir' : snapdir
                        , 'servername' : tsm_servername } # value specified in dsm.sys }
    #inst_cmd='/usr/local/bin/qq.bash'
    my_logger.debug('backup zfs (%s) with cmd (%s)' % (zfsdir, inst_cmd) )
    proc=s.Popen(inst_cmd, stdout=s.PIPE, stderr=s.PIPE, shell=True, cwd='/')
    def stop_backup_directory():
        my_logger.info('stop_backup_directory (%s) on snapshot (%s)' % (zfsdir, snapdir) )
        os.kill(proc.pid, 15) # 15 : SIGTERM
    callback_signal.add_task(stop_backup_directory, 'stop_backup_directory')
    read_set=[proc.stdout, proc.stderr]
    write_set=x_set=[]
    while read_set:
        rlist,wlist,xlist=select.select(read_set, [], [])
        if proc.stdout in rlist:
            stdout=proc.stdout.readline()
            if stdout == '':
                read_set.remove(proc.stdout)
            else:
                my_logger.info("dsmc (out): %s" % stdout.rstrip())
        if proc.stderr in rlist:
            stderr=proc.stderr.readline()
            if stderr == '':
                read_set.remove(proc.stderr)
            else:
                my_logger.info("dsmc (err): %s" % stderr.rstrip())
    callback_signal.del_task('stop_backup_directory')
    
def backup_zpool(zpoolname, tsm_servername):
    #
    # is a zfs FS
    my_logger.info('backup zpool (%s)' % zpoolname)
    my_logger.info('check if zpool (%s) is a zfs FS' % zpoolname)
    if not check_if_zfs(zpoolname):
        my_logger.error('can not backup zpool (%s), because it is not a zpool' % zpoolname)
        raise Exception('can not backup zpool (%s), because it is not a zpool' % zpoolname)
    #
    # lock
    lock.it(zpoolname)
    #
    # snapshot zpool
    my_logger.info('snapshot (%s), and its childs pool' % zpoolname)
    zfsallsnap(zpoolname)
    def remove_snapshot():
        my_logger.info('remove snapshot (%s), and its childs pool' % zpoolname)
        zfsremoveallsnap(zpoolname)
    callback_signal.add_task(remove_snapshot,'remove_snapshot')
    #
    # construct the list of zfs FS to backup
    my_logger.info('construct list of zfs to backup')
    lzfsdir_snapdir=construct_lzfs_to_backup(zpoolname)
    #
    # backup it
    for zfsdir_snapdir in lzfsdir_snapdir:
        my_logger.info('backup dir %s' % zfsdir_snapdir[0])
        backup_directory(zfsdir_snapdir[0], zfsdir_snapdir[1], tsm_servername)
    #
    # remove snapshot zpool
    my_logger.info('remove snapshot (%s), and its childs pool' % zpoolname)
    zfsremoveallsnap(zpoolname)
    callback_signal.del_task('remove_snapshot')
    my_logger.info('zpool (%s) backuped' % zpoolname)
    #
    # unlock it
    lock.remove()


if '__main__' == __name__:
    parser = OptionParser(usage="%prog [-vn] backup zpool_name ...")
    parser.add_option("-v", "--file", action="store_true", dest="isverbose", default=False)
    parser.add_option("-n", "--dryrun", action="store_true", dest="isdryrun", default=False)
    (options, args) = parser.parse_args()
    lzpoolname=[] # if empty it means that we must backup every zones
    action=None
    if options.isverbose:
        VERBOSE=True
    if options.isdryrun:
        DRYRUN=True
    if len(args) > 1:
        if args[0] == 'backup':
            action='backup'
            lzpoolname=args[1:]            
    if not action:
        parser.print_help()
        sys.exit(1)
    elif action == 'backup':
        for zpoolname in lzpoolname:
            backup_zpool(zpoolname, zpoolname)    
        sys.exit()
