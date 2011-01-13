#!/usr/bin/env python

import gdata.docs.data
from gdata.docs.data import MIMETYPES
import gdata.docs.client
from gdata.gauth import ClientLoginToken

from signal import SIGTERM
from getpass import getpass
import sys, os, time, atexit, argparse
from os import mkdir
from os import remove
from os.path import expanduser
from os.path import isdir

import pyinotify
import sqlite3

class Daemon(object):
    """
    A generic daemon class

    Usage: subclass the Daemon class and override the run method
    """
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def daemonize(self):
        """
        do the UNIX double-fork magic, see Stevens' "Advanced Programming in the
        UNIX Environment" for details (ISBN 0201563177)
        http://www.erlenstar.demon.co.uk/unix/faq_2.html#SEC16
        """
        try:
            pid = os.fork()
            if pid > 0:
                #exit first parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        # decouple from parent environment
        os.chdir("/")
        os.setsid()
        os.umask(0)

        # do second fork
        try:
            pid = os.fork()
            if pid > 0:
                # exit from second parent
                sys.exit(0)
        except OSError, e:
            sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
            sys.exit(1)

        #redirect standard file descriptors
        sys.stdout.flush()
        sys.stderr.flush()
        si = file(self.stdin, 'r')
        so = file(self.stdout, 'a+')
        se = file(self.stderr, 'a+', 0)
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        #write pid file
        atexit.register(self.delpid)
        pid = str(os.getpid())
        file(self.pidfile, 'w+').write("%s\n" % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        """
        Start the daemon
        """
        # Check for a pidfile to see if the daemon already runs
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = "pidfile %s already exists. Daemon already running?\n"
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

        # Start the Daemon
        self.daemonize()
        self.run()

    def stop(self):
        """
        Stop the daemon
        """
        # get the pid from the pidfile
        try:
            pf = file(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:
            message = "pidfile %s does not exist. Daemon not running?\n"
            sys.stderr.write(message % self.pidfile)
            return # not an error in a restart

        # Try killing the daemon process
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError, err:
            err = str(err)
            if err.find("No such process") > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print str(err)
                sys.exit(1)

    def restart(self):
        """
        Restart the daemon
        """
        self.stop()
        self.start()

    def run(self):
        """
        You should override this method when you subclass Daemon. It will be called after the process has been
        daemonized by start() or restart().
        """

class DocDb(object):
    db = None
    
    def __init__(self):
        pass
    
    def _initDb(self):
        CREATE_TOKEN_TABLE = '''create table token
        (token text, id int primary key)
        '''
        CREATE_DOCS_TABLE = '''create table docs
        (local_path text, resource_id text primary key, etag text, title text)
        '''
        
        try:
            self.db.execute(CREATE_TOKEN_TABLE)
        except sqlite3.OperationalError, error:
            pass
            
        try:
            self.db.execute(CREATE_DOCS_TABLE)
        except sqlite3.OperationalError:
            pass
    
    def setDb(self, db_file):
        self.db_file = db_file
        self.db = sqlite3.connect(self.db_file, isolation_level=None)
        self._initDb()
        
    def getToken(self):
        query = "SELECT token FROM token WHERE id = 1"
        res = self.db.execute(query).fetchone()
        if res:
            return res[0]
        return False
        
    def saveToken(self, token):
        query = "INSERT OR REPLACE INTO token (token, id) VALUES (?, 1)"
        self.db.execute(query, (token,))
        
    def addDoc(self, doc, path):
        query = "INSERT OR REPLACE INTO docs (local_path, resource_id, etag, title) VALUES (?, ?, ?, ?)"
        self.db.execute(query, (path, doc.resource_id.text, doc.etag, doc.title.text))
        
    def getEtag(self, resource_id):
        query = "SELECT etag FROM docs WHERE resource_id = ?"
        res = self.db.execute(query, (resource_id,)).fetchone()
        if res:
            return res[0]
        return False
        
    def getRowFromPath(self, path):
        query = "SELECT resource_id, etag, title FROM docs WHERE local_path = ?"
        res = self.db.execute(query, (path,)).fetchone()
        return res
        
    def resetEtag(self, doc):
        query = "UPDATE docs SET etag = ? WHERE resource_id = ?"
        self.db.execute(query, (doc.etag, doc.resource_id.text))

class DocSync(object):
    client = gdata.docs.client.DocsClient(source='GreggoryHernandez-DocSync-v1')
    home = expanduser('~')
    gdocs_folder = home +'/Google Docs'
    db_file = gdocs_folder +'/.db'
    db = DocDb()
    authd = False
    
    def __init__(self):
        self.client.ssl = True
        self.client.http_client_debug = False
        self.createBaseFolder()
        
    def createBaseFolder(self):
        if not isdir(self.gdocs_folder):
            mkdir(gdocs_folder, 0755)
            
    def start(self):
        self._authorize()
        self._getEverything()
        self._watchFolder()
        
        self._setPeriodicSync()
        
        print 'started'
        
    def _watchFolder(self):
        wm = pyinotify.WatchManager()
        wm.add_watch(self.gdocs_folder, pyinotify.IN_MODIFY, rec=True)
        handler = EventHandler(self)
        notifier = pyinotify.ThreadedNotifier(wm, handler)
        notifier.start()
        
    def _authorize(self):
        self.db.setDb(self.db_file)
        token = self.db.getToken()
        if not token:
            username = raw_input('Username: ')
            password = getpass()
            self.client.ClientLogin(username, password, self.client.source)
            self.db.saveToken(self.client.auth_token.token_string)
        else:
            self.client.auth_token = ClientLoginToken(token)
            
        self.authd = True
            
    def _getEverything(self):
        self.db.setDb(self.db_file)
        docs = self.client.GetEverything(uri='/feeds/default/private/full/-/document')
        for doc in docs:
            path = self.gdocs_folder +'/'+ doc.title.text.replace('/', '-') +'.odt'
            try:
                if doc.etag != self.db.getEtag(doc.resource_id.text):
                    print 'writing:', path
                    self.client.Export(doc, path)
                    self.db.addDoc(doc, path)
                else:
                    pass
            except:
                print 'skipped:', path
                remove(path)
        
    def updateDoc(self, path):
        self.db.setDb(self.db_file)
        
        if not self.authd:
            self._authorize()
        
        db_row = self.db.getRowFromPath(path)
        if not db_row:
            return False
        
        resource_id = db_row[0]
        etag = db_row[1]
        title = db_row[2]
        
        ms = gdata.data.MediaSource(file_path=path, content_type=MIMETYPES['ODT'])
        doc = self.client.GetDoc(resource_id.replace(':', '%3A'))
        new_version = self.client.Update(doc, media_source=ms)
        print 'Document pushed:', new_version.GetAlternateLink().href
        
        self.db.resetEtag(new_version)
        
class EventHandler(pyinotify.ProcessEvent):
    def __init__(self, syncer):
        pyinotify.ProcessEvent.__init__(self)
        self.syncer = syncer

    def runCommand(self, event):
        self.syncer.db.setDb(self.syncer.db_file)
        self.syncer.updateDoc(event.path + '/' + event.name)

    def process_IN_MODIFY(self, event):
        self.runCommand(event)
        
    def process_IN_CLOSE_WRITE(self, event):
        self.runCommand(event)
        
class SyncDaemon(Daemon):
    def __init__(self):
        self.stdin   = '/dev/null'
        self.stdout  = '/tmp/sync.log'
        self.stderr  = '/tmp/sync.log'
        self.pidfile = '/tmp/sync.pid'
    
    def run(self):
        sync = DocSync()
        sync.start()
        #sync.updateDoc('/home/gregg/Google Docs/SampleDoc.odt')

if __name__ == "__main__":
    daemon = SyncDaemon()
    parser = argparse.ArgumentParser()
    
    parser.add_argument('command',
                        action='store',
                        choices=['start','stop','restart','debug'],
                        help='What to do. Use debug to start in the foreground')
    
    args = parser.parse_args()
    
    # Execute the command
    if 'start' == args.command:
        daemon.start()
    elif 'stop' == args.command:
        daemon.stop()
        print 'stoped'
    elif 'restart' == args.command:
        daemon.restart()
    elif 'debug' == args.command:
        daemon.run()
    else:
        print "Unkown Command"
        sys.exit(2)
    sys.exit(0)

