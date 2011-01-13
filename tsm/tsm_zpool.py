#!/usr/bin/python
# 22.12.2010 cED add --no-email options
# 22.12.2010 cED add discovering new zpool and place a dsm.sys template in it
# 22.12.2010 cED add reading of <zone_root>/etc/aliases
# 22.12.2010 cED add management of interuption in mail
# 21.12.2010 cED add management of dsm
# 14.12.2010 cED add management of the lock
# 14.12.2010 cED add management of the interruption
# 14.12.2010 cED add logging systems
# 15.12.2010 cED catch ANS????? errors and treat them
# TODO: length time to backup
# TODO: put a file in the zone and in the master which gives info to be able to backup also within the zone
# TODO: implement option --keep-snapshot
# TODO: implement option --dry-run


import os, sys, shutil
from optparse import OptionParser
from datetime import datetime
import subprocess as s
import logging
import signal
import select
import re
import socket # for hostname

SNAPNAME_PREFIX='tsmzpool' 
TMPDIR='/tmp/tsm_zpool'
VERBOSE=False
DRYRUN=False
VAR_DIR='/var/run'
LOG_FILENAME='/var/log/tsm_zpool'
DSM_SYS_FILENAME='/opt/tivoli/tsm/client/ba/bin/dsm.sys'
MEL_SENDER='unix-noreply@unige.ch'
LEMAIL_ROOT=['unix-bot@unige.ch']

ZFS_LIST_CMD="zfs list -H -o name -t filesystem"
ZFSALLSNAP_CMD="/usr/local/bin/zfsallsnap snapshot --backup --clobber %(zonename)s@%(snapname)s"
ZFSREMOVEALLSNAP_CMD="/usr/local/bin/zfsallsnap destroy %(zonename)s@%(snapname)s"
MOUNT_CMD='/usr/sbin/mount'
DSMC_BACKUP='dsmc incr -servername=%(servername)s %(zfsdir)s -snapshotroot=%(snapdir)s'
DSMC_RESTORE='dsmc restore -servername=tstore1_pool -subdir=yes -preservepath=subtree /zones/tstore1/var/ /root_pool/restore_tstore1/var/'
ZPOOL_LIST_CMD='zpool list -H -o name'
ZFS_GET_CH_UNIGE_ZONEPATHS_CMD='zfs get -Ho name,value ch.unige:zonepaths %(lzfsname_with_space)s'
RE_DSMC_ERROR=re.compile('ANS\S{5}')
L_DSMC_ERROR_OK=['ANS1898I']
PID=os.getpid()
DATE_FORMAT='%Y.%m.%d-%H:%M'
KEEP_SNAPSHOT=False # this is used in the options of tsm_zpool
STR_NEW_DSM_SYS='''*
* dsm.sys
*
* to enable the backup for this zpool
*   - ask the san guys to create a new account, and copy in the email this
*     dsm.sys
*   - change accordingly to the response of the SAN guys, the fields 
*     (TCPPort and TCPServeraddress) of this file
*   - uncomment the lines after the line :
*     ***** uncomment the line under this line to put this file active ******
*   - run once :
*     tsm_zpool generate_dsm
*     to insert the new dsm.sys in the global dsm.sys :
*     %(dsm_sys)s
*   - register the password in dsmc by running :
*     dsmc --servername=%(servername)s
*   - backup it with :
*     tsm_zpool backup %(servername)s
*
*
***** uncomment the line under this line to put this file active ******
*
*SErvername      %(servername)s
*    COMMMethod          TCPip
*    TCPPort             1506
*    TCPServeraddress    sos6.unige.ch
*    NODENAME            mail-tstore1_pool
*    PASSWORDACCESS      generate
*    MAXCMDRETRIES 6
*    RETRYPERIOD 10
*    ERRORLOGNAME /var/log/dsmerror.log
*    ERRORLOGRETENTION 10
*    SCHEDLOGNAME /var/log/dsmsched.log
*    SCHEDLOGRETENTION 7
*    POSTSchedulecmd "echo 'TSM Backup : fin du backup. Le log se trouve dans : /var/log/dsmsched.log' | mailx -s 'TSM Backup' %(str_lemail)s"
'''
#
# logging stuff
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)

my_logger = logging.getLogger('MyLogger')
my_logger.setLevel(logging.INFO)
#my_logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler(LOG_FILENAME)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s") )
my_logger.addHandler(file_handler)



def to_stdout():
    stream_handler = logging.StreamHandler(sys.stdout)
    my_logger.addHandler(stream_handler)

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
        my_logger.debug('CTRL-C or SIGTERM catched')
        for taskname, task in reversed (self.ltask):
            my_logger.info('launch task (%s)' % taskname)     
            task()        
callback_signal=CallbackSignal()
signal.signal(signal.SIGINT, callback_signal.termination)
signal.signal(signal.SIGTERM, callback_signal.termination)

def cmpAlphaNum(str1,str2):
   str1=str1.lower()
   str2=str2.lower()
   ReSplit='(\d+)'
   str1=re.split(ReSplit,str1)
   str2=re.split(ReSplit,str2)
   if( ''==str1[0] ):
      str1.remove('')
   if( ''==str1[len(str1)-1] ):
      str1.remove('')
   if( ''==str2[0] ):
      str2.remove('')
   if( ''==str2[len(str2)-1] ):
      str2.remove('')
   for i in range( min( len(str1),len(str2) ) ):
      try:
         tmp=int(str1[i])
         str1[i]=tmp
      except:ValueError
      try:
         tmp=int(str2[i])
         str2[i]=tmp
      except:ValueError
      if( str1[i]==str2[i] ):
         continue
      if (str1[i]>str2[i]):
         return 1
      else:
         return -1
   return cmp(len(str1),len(str2))

def send_email(sender, recipient, subject, body):
   from smtplib import SMTP
   from email.MIMEText import MIMEText
   from email.Header import Header
   from email.Utils import parseaddr, formataddr
   # Header class is smart enough to try US-ASCII, then the charset we
   # provide, then fall back to UTF-8.
   header_charset = 'ISO-8859-1'
   # We must choose the body charset manually
   for body_charset in 'UTF-8', 'ISO-8859-1', 'US-ASCII'  :
      try:
         body.encode(body_charset)
      except UnicodeError:
         pass
      else:
         break
   # Split real name (which is optional) and email address parts
   sender_name, sender_addr = parseaddr(sender)
   recipient_name, recipient_addr = parseaddr(recipient)
   # We must always pass Unicode strings to Header, otherwise it will
   # use RFC 2047 encoding even on plain ASCII strings.
   sender_name = str(Header(unicode(sender_name), header_charset))
   recipient_name = str(Header(unicode(recipient_name), header_charset))
   # Make sure email addresses do not contain non-ASCII characters
   sender_addr = sender_addr.encode('ascii')
   recipient_addr = recipient_addr.encode('ascii')
   # Create the message ('plain' stands for Content-Type: text/plain)
   msg = MIMEText(body.encode(body_charset), 'plain', body_charset)
   msg['From'] = formataddr((sender_name, sender_addr))
   msg['To'] = formataddr((recipient_name, recipient_addr))
   msg['Subject'] = Header(unicode(subject), header_charset)
   # Send the message via SMTP to localhost:25
   smtp = SMTP("localhost")
   smtp.sendmail(sender, recipient, msg.as_string())
   smtp.quit()


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

class NotifyError(object):
    def __init__(self):
        self.demail_error={}
        self.email_disabled=False
#    def add(self, zpool_conf, zfsdir, stdout):
    def add(self, lemail, msg_or_lmsg):
        if not self.demail_error:
            callback_signal.add_task(self.send_interrupted, 'send_email_notification' )
        #
        if type(msg_or_lmsg) != list:
            msg_or_lmsg=[msg_or_lmsg]
        for email in lemail:
            email=email.lower()
            for msg in msg_or_lmsg:
                if self.demail_error.get(email):
                    self.demail_error[email].append(msg)
                else:
                    self.demail_error[email]=[msg]
    def send(self, was_interrupted=False):
        if self.email_disabled:
            self.demail_error={}
        hostname=socket.gethostname()
        for email, lmsg in self.demail_error.iteritems():
            sender=MEL_SENDER
            recipient=email
            subject='[tsm_zpool] from host (%s)' % hostname
#            recipient='cedric.briner@unige.ch'; subject+=' email (%s)'% email # 4debug
            if was_interrupted:
                body="tsm_zpool was interrupted\n\n"+os.linesep.join(lmsg)
            else:
                body=os.linesep.join(lmsg)
            send_email(sender, recipient, subject, body)
        if self.demail_error:
            my_logger.info( 'notifications sent by email to [%s]' % ', '.join(self.demail_error.keys()) )
        else:
            my_logger.info( 'no notification to send by email')
    def send_interrupted(self):
        self.send(was_interrupted=True)
    def disable_email(self):
        my_logger.info( 'email notification disabled')
        self.email_disabled=True
        
notify_error=NotifyError()

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
    now=datetime.now()
    str_now=now.strftime(DATE_FORMAT)
    snapname='%s.%s.%s' % (SNAPNAME_PREFIX,str_now, str(PID)  )
    inst_cmd=ZFSALLSNAP_CMD % {'zonename': zonename, 'snapname': snapname }
    my_logger.debug('zfsallsnap cmd:'+inst_cmd)
    proc=s.Popen(inst_cmd, stdout=s.PIPE, stderr=s.PIPE, shell=True, cwd='/')
    stdout, stderr = proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        my_logger.error('the cmd (%s) did not succeed' % inst_cmd)
        raise Exception( 'zfsallsnap problem')
    return snapname

def zfsremoveallsnap(zonename, snapname):
    if KEEP_SNAPSHOT:
        my_logger.info('do not remove the snapshot for the zone (%s)' % zonename)
        return
    inst_cmd=ZFSREMOVEALLSNAP_CMD % {'zonename': zonename, 'snapname': snapname }
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

def construct_lzfs_to_backup(zpoolname, snapname):
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
            my_logger.debug("zfs (%s) doesn't have a mountpoint" % zfs)
            continue
        #check that the mountpoint of the snapshot is there
        snapshot_dirpath=zfs_dirpath+'/.zfs/snapshot/'+snapname
        try:
            os.stat(snapshot_dirpath)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            my_logger.error("zfs (%s) doesn't have a mountpoint (%s) for the snapshot" % (zfs, snapshot_dirpath))
            continue
        lzfsdir_snapdir.append([zfs_dirpath, snapshot_dirpath])
    return lzfsdir_snapdir

class ZpoolConf(object):
    def __init__(self, name, ch_unige_zonepaths=None, lemail_root=LEMAIL_ROOT):
        self.name=name
        if ch_unige_zonepaths:
            self.lch_unige_zonepath=ch_unige_zonepaths.split(' ')
        else:
            self.lch_unige_zonepath=[]
        self.lemail_root=lemail_root
        self.dsm_sys=None
    def __repr__(self):
        return self.__class__.__name__+':'+self.name

class DsmSys(object):
    RE_COMMENT=re.compile('\s*\*')
    RE_BLANK=re.compile('\s*$')
    RE_SERVERNAME=re.compile('\s*SE\S*\s+(\S*)')
    RE_PARAMETERS=re.compile('\s*(\S+)\s+(\S+)')
    def __init__(self, path=DSM_SYS_FILENAME):
        self.dservername={}
        self.path=path
        self.construct(path)
    def _get_lservername(self):
        ret=self.dservername.keys()
        ret.sort()
        return ret
    lservername = property(_get_lservername)
    def get_unique_servername(self):
        if len(self.lservername) == 0:
            return None
        elif len(self.lservername) == 1:
            return self.lservername[0]
        else:
            msg='DsmSys object contruct from (%s) should have only one servername' % self.path
            my_logger.warning(msg)
            raise Exception(msg)
    def construct(self, fnpath):
        fhpath=open(fnpath,'r')
        self.lline=fhpath.readlines()
        fhpath.close()
        for line in self.lline:
            if self.RE_COMMENT.match(line) or self.RE_BLANK.match(line):
                continue
            match=self.RE_SERVERNAME.match(line)
            if match:
                servername=match.groups()[0]
                self.dservername[servername]={'servername':servername,'lline':[line.rstrip()]}
                self.dservername[servername]['lparameter_value']=[]
                continue
            match=self.RE_PARAMETERS.match(line)
            if match:
                parameter,value=match.groups()
                self.dservername[servername]['lparameter_value'].append( (parameter, value) )
                self.dservername[servername]['lline'].append(line.rstrip())
    def __repr__(self):
        ll=[]
        ll.append( 'path: %s' % self.path)
        lservername=self.lservername
        lservername.sort(cmpAlphaNum)
        for servername in lservername:
            ll.append(' servername:%s' % servername )
            ll.append('  lparameter_value')
            for parameter, value in self.dservername[servername]['lparameter_value']:
                ll.append('   %s %s' %(parameter, value) )
            ll.append('  lline')
            for line in self.dservername[servername]['lline']:
                ll.append('   %s' % line )
        return os.linesep.join(ll)
    def __str__(self):
        ll=[]
        lservername=self.lservername
        lservername.sort(cmpAlphaNum)
        for servername in lservername:
            for line in self.dservername[servername]['lline']:
                ll.append(line)
        return os.linesep.join(ll)
    def compare_with(one, two, servername):
        #
        #check that both dsm.sys have an entry with the servername
        if not (one.dservername.has_key(servername) and two.dservername.has_key(servername) ):
            return -1
        one_lparameter_value=one.dservername[servername]['lparameter_value']
        two_lparameter_value=two.dservername[servername]['lparameter_value']
        #
        # put it in other construction to help the comparaison
        d1_p={}
        for p,v in one_lparameter_value:
            d1_p[p.upper()]=(p,p.upper(),v)
        d2_p={}        
        for p,v in two_lparameter_value:
            d2_p[p.upper()]=(p,p.upper(),v)
        #
        # check that both have the same list of parameter
        if [P for p,P,v in d1_p.itervalues()] != [P for p,P,v in d2_p.itervalues()]:
            return -2
        #
        # check that the corresponding values are the same
        for k in d1_p.keys():
            if d1_p[k][2] != d2_p[k][2]:
                return -3
        return 0
    def get_if_servername_is_backupable(self, servername=None):
        if servername==None:
            servername=self.get_unique_servername
            if not servername:
                return False
        ddata=self.dservername.get(servername)
        if ddata:
            return True
        return False

def generate_dsm_n_get_dzpool_conf():
    lmessage_4_new_dsm_sys=[]
    my_logger.debug('enter in "collect_dsm_conf"')
    #
    # get lzpoolname
    proc=s.Popen(ZPOOL_LIST_CMD, stdout=s.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % ZPOOL_LIST_CMD)
    dzpool_conf={}
    for out in lout:
        zpoolname=out.rstrip()
        dzpool_conf[zpoolname]=ZpoolConf(zpoolname)
    if not dzpool_conf:
        return {}  
    #
    # get ch.unige:zonepaths 
    cmd=ZFS_GET_CH_UNIGE_ZONEPATHS_CMD % { 'lzfsname_with_space' :' '.join(dzpool_conf.keys()) }
    proc=s.Popen(cmd, stdout=s.PIPE, shell=True, cwd='/')
    retcode=proc.wait()
    lout=proc.stdout.readlines()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % cmd)
    for out in lout:
        # out = davtst1_pool    /zones/davtst1_pool/davtst1
        #       root_pool       -
        zpoolname,ch_unige_zonepaths=out.rstrip().split('\t')
        if ch_unige_zonepaths == '-':
            pass
        else:
            dzpool_conf[zpoolname].lch_unige_zonepath=ch_unige_zonepaths.split(' ')
    #
    # get lemail_root
    for zpool_conf in dzpool_conf.itervalues():
        if not zpool_conf.lch_unige_zonepath:
            continue
        for ch_unige_zonepath in zpool_conf.lch_unige_zonepath:
            fn_etc_alias=os.path.join(ch_unige_zonepath,'root/etc/aliases')
            if not os.path.isfile(fn_etc_alias):
                zpool_conf.lemail_root=[]
            else:
                fh_etc_alias=open(fn_etc_alias,'r')
                for line in fh_etc_alias.readlines() :
                    if line.find('root:') == 0:
                        zpool_conf.lemail_root=[email.rstrip().lstrip() for email in line[len('root:'):].split(',') if -1 != email.find('@')]
                        continue
    #
    # create dsm.sys on Virtual Node
    #   the idea is to create it where there is already an zone.cfg
    for zpool_conf in dzpool_conf.itervalues():
        if not zpool_conf.lch_unige_zonepath:
            continue
        must_create_dsm_sys=True
        fn_new_dsm_sys=''
        for zonepath in zpool_conf.lch_unige_zonepath:
            fn_dsm_sys=os.path.join(zonepath,'dsm.sys')
            if os.path.isfile(fn_dsm_sys):
                must_create_dsm_sys=False
                continue
            fn_zone_cfg=os.path.join(zonepath, 'zone.cfg')
            if os.path.isfile(fn_zone_cfg):
                can_create_dsm_sys=True
                fn_new_dsm_sys=fn_dsm_sys
        if fn_new_dsm_sys and must_create_dsm_sys:
            fh_dsm_sys=open(fn_new_dsm_sys, 'w')
            lemail=list( set(LEMAIL_ROOT).union(set(zpool_conf.lemail_root)) )
            str_lemail=','.join(lemail)
            fh_dsm_sys.write(STR_NEW_DSM_SYS % {'servername':zpool_conf.name
                                               ,'dsm_sys': DSM_SYS_FILENAME
                                               ,'str_lemail': str_lemail})
            fh_dsm_sys.close()
            lemail=list( set(LEMAIL_ROOT).union(set(zpool_conf.lemail_root)) )
            notify_error.add(lemail, 'new zpool discovered, edit dsm.sys (%s), and configure it' % fn_new_dsm_sys)
    #
    # get dsm.sys on Virtual Node
    for zpool_conf in dzpool_conf.itervalues():
        if not zpool_conf.lch_unige_zonepath:
            continue
        for zonepath in zpool_conf.lch_unige_zonepath:
            # get
            fn_dsm_sys=os.path.join(zonepath,'dsm.sys')
            if os.path.isfile(fn_dsm_sys):
                zpool_conf.dsm_sys=DsmSys(fn_dsm_sys)
                continue
    #
    # get dsm.sys on physical node
    if os.path.isfile(DSM_SYS_FILENAME):
        dsm_sys_global=DsmSys(DSM_SYS_FILENAME)
    else:
        dsm_sys_global=None
    #
    # check dsm.sys collected are the same as the global dsm.sys
    must_create_new_dsm_sys=False
    for servername in dsm_sys_global.dservername.keys():
        #
        # check that no zpool get moved away
        if not dzpool_conf.get(servername):
            msg="old dsm.sys with servername (%s) not available anymore, it was removed from (%s)" % (servername, DSM_SYS_FILENAME)
            notify_error.add(LEMAIL_ROOT, msg)
            lmessage_4_new_dsm_sys.append(msg)
            my_logger.info(msg)
            must_create_new_dsm_sys=True
            continue
        #
        #check that dsm.sys for a servername has the same parameters
        dsm_sys_virtual=dzpool_conf.get(servername).dsm_sys
        if dsm_sys_virtual:
            ret_cmp=dsm_sys_global.compare_with(dsm_sys_virtual, servername)
            if ret_cmp != 0:
                msg="dsm.sys (%s) differ from (%s) for the entry (%s)" % (dsm_sys_virtual.path, DSM_SYS_FILENAME, servername)
                lemail=list( set(LEMAIL_ROOT).union(set(dzpool_conf[servername].lemail_root)) )
                notify_error.add(lemail, msg)
                lmessage_4_new_dsm_sys.append(msg)
                my_logger.info(msg)
                must_create_new_dsm_sys=True
    #
    #check if a new zpool came back
    lservername_virtual=[]
    for servername in dzpool_conf.keys():
        if dzpool_conf[servername].dsm_sys:
            if dzpool_conf[servername].dsm_sys.get_if_servername_is_backupable():
                lservername_virtual.append(servername)
    lnew_servername=list( set(lservername_virtual) - set(dsm_sys_global.dservername.keys()) )
    if lnew_servername:
        msg="new dsm.sys in (%s) is available, it was added to (%s)" % (', '.join(lnew_servername), DSM_SYS_FILENAME)
        lemail=list( set(LEMAIL_ROOT).union(set(dzpool_conf[servername].lemail_root)) )
        notify_error.add(lemail, msg)
        lmessage_4_new_dsm_sys.append(msg)
        my_logger.info(msg)
        must_create_new_dsm_sys=True
    #
    # if needed then create the new dsm_sys
    if must_create_new_dsm_sys:
        dt_dsm_sys=datetime.fromtimestamp(os.stat(DSM_SYS_FILENAME)[8])
        fn_new_dsm_sys=DSM_SYS_FILENAME+'.'+dt_dsm_sys.strftime('%Y.%m.%d-%H:%M')
        os.rename(DSM_SYS_FILENAME, fn_new_dsm_sys)
        fh=open(DSM_SYS_FILENAME, 'w')
        lzpoolname=dzpool_conf.keys()
        lzpoolname.sort(cmpAlphaNum)
        fh.write('* list of reason why this version was created\n')
        for message in lmessage_4_new_dsm_sys:
            fh.write('* '+message+'\n')
        for zpoolname in lzpoolname:
            if dzpool_conf[zpoolname].dsm_sys:
                fh.write(os.linesep)
                dsm_sys=dzpool_conf[zpoolname].dsm_sys
                fh.write('* from %s\n' % dsm_sys.path)
                fh.write( str(dsm_sys) )
                fh.write(os.linesep)
        fh.close()            
        my_logger.info("new dsm.sys (%s) created, older version kept on (%s)" % (DSM_SYS_FILENAME, fn_new_dsm_sys) )
        notify_error.add(LEMAIL_ROOT, msg)
    dzpool_conf_backupable={}
    for servername, zpool_conf in dzpool_conf.iteritems():
        if zpool_conf.dsm_sys:
            dzpool_conf_backupable[servername]=zpool_conf
    return dzpool_conf_backupable

#def backup_directory(zfsdir, snapdir, tsm_servername):
def backup_directory(zfsdir, snapdir, zpool_conf):
    tsm_servername=zpool_conf.dsm_sys.get_unique_servername()
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
                stdout=stdout.rstrip()
                re_ret=RE_DSMC_ERROR.search(stdout)
                is_ok=True
                if re_ret:
                    if re_ret.group() not in L_DSMC_ERROR_OK:
                        lbody_email=['dsm.sys in (%s)' % zpool_conf.dsm_sys.path
                                    ,'zfsdir (%s) - %s' % (zfsdir, stdout)]
                        notify_error.add(zpool_conf.lemail_root, lbody_email)
                        my_logger.error("dsmc (out): %s" % stdout)
                        is_ok=False
                if is_ok:
                    my_logger.debug("dsmc (out): %s" % stdout)
        if proc.stderr in rlist:
            stderr=proc.stderr.readline()
            if stderr == '':
                read_set.remove(proc.stderr)
            else:
                stderr=stderr.rstrip()
                re_ret=RE_DSMC_ERROR.search(stderr)
                is_ok=True
                if re_ret:
                    if re_ret.group not in L_DSMC_ERROR_OK :
                        lbody_email=['dsm.sys in (%s)' % zpool_conf.dsm_sys.path
                                    ,'zfsdir (%s) - %s' % (zfsdir, stderr)]
                        notify_error.add(zpool_conf.lemail_root, lbody_email)
                        my_logger.error("dsmc (err): %s" % stderr)
                        is_ok=False
                if is_ok:
                    my_logger.info("dsmc (err): %s" % stderr)
    callback_signal.del_task('stop_backup_directory')
    
#def backup_zpool(zpoolname, tsm_servername):
def backup_zpool(zpool_conf):
    #
    # is a zfs FS
    my_logger.info('backup zpool (%s)' % zpool_conf.name)
    my_logger.info('check if zpool (%s) is a zfs FS' % zpool_conf.name)
    if not check_if_zfs(zpool_conf.name):
        msg='can not backup zpool (%s), because it is not a zpool' % zpool_conf.name
        my_logger.error(msg)
        raise Exception(msg)
    #
    # lock
    lock.it(zpool_conf.name)
    #
    # snapshot zpool
    my_logger.info('snapshot (%s), and its childs pool' % zpool_conf.name)
    snapname=zfsallsnap(zpool_conf.name)
    def remove_snapshot():
        my_logger.info('remove snapshot (%s), and its childs pool' % zpool_conf.name)
        zfsremoveallsnap(zpool_conf.name, snapname)
    callback_signal.add_task(remove_snapshot,'remove_snapshot')
    #
    # construct the list of zfs FS to backup
    my_logger.info('construct list of zfs to backup')
    lzfsdir_snapdir=construct_lzfs_to_backup(zpool_conf.name, snapname)
    #
    # backup it
    for zfsdir_snapdir in lzfsdir_snapdir:
        my_logger.info('backup dir %s' % zfsdir_snapdir[0])
        backup_directory(zfsdir_snapdir[0], zfsdir_snapdir[1], zpool_conf)
    #
    # remove snapshot zpool
    my_logger.info('remove snapshot (%s), and its childs pool' % zpool_conf.name)
    zfsremoveallsnap(zpool_conf.name, snapname)
    callback_signal.del_task('remove_snapshot')
    my_logger.info('zpool (%s) backuped' % zpool_conf.name)
    #
    # unlock it
    lock.remove()

if '__main__' == __name__:
    parser = OptionParser(usage='''%prog [-vn] backup [zpool_name  [...] ]
       %prog [-vn] generate_dsm''')
    parser.add_option("-v", "--file"  , action="store_true", dest="isverbose", default=False)
    parser.add_option("-n", "--dryrun", action="store_true", dest="isdryrun", default=False, help='NOT IMPLEMENTED')
    parser.add_option("--no-email", action="store_false", dest="send_email", default=True, help='do not send any email')
    parser.add_option("--keep-snapshot", action="store_true", dest="keep_snapshot", default=False, help='do not remove snapshot after the backup (usefull to test recover)')
    (options, args) = parser.parse_args()
    lzpoolname=[] # if empty it means that we must backup every zones
    laction=[]
    if not options.send_email:
        notify_error.disable_email()
    if options.isverbose:
        VERBOSE=to_stdout()
    if options.isdryrun:
        DRYRUN=True
    if len(args) == 1:
        if args[0] == 'generate_dsm':
            laction='[generate_dsm]'
        if args[0] == 'backup':
            laction='[generate_dsm, backup]'
            lzpoolname=[]
    if len(args) > 1:
        if args[0] == 'backup':
            laction='[generate_dsm, backup]'
            lzpoolname=args[1:]            
    if not laction:
        parser.print_help()
        sys.exit(1)
    if options.keep_snapshot:
        my_logger.info('option keep_snapshot enable')
        KEEP_SNAPSHOT=True
    if 'generate_dsm' in laction:
        dzpool_conf=generate_dsm_n_get_dzpool_conf()
    if 'backup' in laction:
        #
        # restrict the dictionnary of dzpool_conf, to the one specifed in the cli
        dzpool_conf_to_backup={}
        if lzpoolname:
            for zpoolname in lzpoolname:
                isbackupable=False
                if dzpool_conf.get(zpoolname):
                    if dzpool_conf[zpoolname].name == zpoolname:
                        dzpool_conf_to_backup[zpoolname]=dzpool_conf[zpoolname]
                        isbackupable=True
                if not isbackupable:
                    my_logger.warning('can not backup zpool (%s)' % zpoolname)
        else:
            dzpool_conf_to_backup=dzpool_conf
        #
        # do the backup for this (zpoolname)
        for zpoolname, zpool_conf in dzpool_conf_to_backup.iteritems():
            if zpool_conf.dsm_sys.get_if_servername_is_backupable(zpoolname):
                backup_zpool(zpool_conf)
    #
    # send notification
    notify_error.send()
    sys.exit()
