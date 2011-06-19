#!/usr/bin/env python

import gdata.docs.data
from gdata.docs.data import MIMETYPES
import gdata.docs.client
from gdata.gauth import ClientLoginToken

from signal import SIGTERM
from getpass import getpass
import sys, os, time, atexit, argparse

import pyinotify
import sqlite3
from daemon import Daemon

class DocDb(object):
    """
    Interface for interacting with the local sqlite databse
    """
    
    db = None # Holds db connection once setDb is called
    
    def __init__(self):
        """
        nothing to be done here just yet. we're not initializing the database
        here so that users of this class can control when and how often the
        connection is initialized. This needed to be this way to allow some
        threading since you can't operate on a connection that was created in
        a different thread.
        
        @TODO: Determine if it might be more useful to instaniate a new object
            each time a connection is needed in a thread.
        """
        pass
    
    def _initDb(self):
        """
        Attempt to create the tables needed in the database. They will only be
        created if they don't already exist. If the database needs to be 
        updated, that should happen here.
        
        @TODO: Set a database version so it can be determined if this codes 
            needs to be run without actually attempting it.
        """
        CREATE_TOKEN_TABLE = '''create table token
        (token text, id int primary key)
        '''
        CREATE_DOCS_TABLE = '''create table docs
        (local_path text, resource_id text primary key, etag text, title text)
        '''
        
        try:
            self.db.execute(CREATE_TOKEN_TABLE)
            self.db.execute(CREATE_DOCS_TABLE)
        except sqlite3.OperationalError, error:
            pass
    
    def setDb(self, db_file):
        """
        Use this method to instantiate the database. This must be called in each
        new thread that is created, otherwise, sqlite will yell at you.
        """
        self.db_file = db_file
        self.db = sqlite3.connect(self.db_file, isolation_level=None)
        self._initDb()
        
    def getToken(self):
        """
        See if the user has previously authenticated and return that token. If
        the user hasn't authenticated, return False.
        """
        query = "SELECT token FROM token WHERE id = 1"
        res = self.db.execute(query).fetchone()
        if res:
            return res[0]
        return False
        
    def saveToken(self, token):
        """
        Save the auth token to the db. The table token should only ever have one
        row where id = 1. This is the user's token. Later this can potentially 
        be used to allow more than one use to be auth'd. But more likely, only
        one user should be auth'd at a time.
        """
        query = "INSERT OR REPLACE INTO token (token, id) VALUES (?, 1)"
        self.db.execute(query, (token,))
        
    def addDoc(self, doc, path):
        """
        Adds or replaces the given doc and associated path to the docs table.
        """
        query = "INSERT OR REPLACE INTO docs (local_path, resource_id, etag, title) VALUES (?, ?, ?, ?)"
        self.db.execute(query, (path, doc.resource_id.text, doc.etag, doc.title.text))
        
    def getEtag(self, resource_id):
        """
        Get the etag by resource_id. Useful to see if the doc has changed on the
        server. Returns False when a document isn't found.
        """
        query = "SELECT etag FROM docs WHERE resource_id = ?"
        res = self.db.execute(query, (resource_id,)).fetchone()
        if res:
            return res[0]
        return False
        
    def getRowFromPath(self, path):
        """
        Get a row from the docs table by the path.
        """
        query = "SELECT resource_id, etag, title FROM docs WHERE local_path = ?"
        res = self.db.execute(query, (path,)).fetchone()
        return res
        
    def resetEtag(self, doc):
        """
        Updates the given doc's etag. Useful after various sync operations.
        """
        query = "UPDATE docs SET etag = ? WHERE resource_id = ?"
        self.db.execute(query, (doc.etag, doc.resource_id.text))

class DocSync(object):
    """
    Handles the business logic of syncing. Every operation of syncing should be 
    performed via an instance of DocSync.
    """
    
    client = gdata.docs.client.DocsClient(source='GreggoryHernandez-DocSync-v1')
    home = os.path.expanduser('~')
    gdocs_folder = home +'/Google Docs'
    db_file = gdocs_folder +'/.db'
    db = DocDb()
    authd = False
    
    def __init__(self):
        """
        just setting stuff up.
        """
        self.client.ssl = True
        self.client.http_client_debug = False
        self.createBaseFolder()
        
    def createBaseFolder(self):
        """
        Make the ~/Google Docs folder exist if it doesn't already.
        """
        if not os.path.isdir(self.gdocs_folder):
            os.mkdir(self.gdocs_folder, 0755)
            
    def start(self):
        """
        proxy method to get everything started.
        """
        #self.authorize()
        self.getEverything()
        self._watchFolder()
        
        #TODO: make this work
        #self._setPeriodicSync()
        
        print 'started'
        
    def _watchFolder(self):
        """
        sets up the watching of the docs folder for changes.
        """
        wm = pyinotify.WatchManager()
        wm.add_watch(self.gdocs_folder, pyinotify.IN_MODIFY, rec=True)
        handler = EventHandler(self)
        notifier = pyinotify.ThreadedNotifier(wm, handler)
        notifier.start()
        
    def authorize(self):
        """
        Make sure the user is authorized. Ask for username/password if needed.
        @TODO: add a UI so that the daemon can get auth info.
        """
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
            
    def getEverything(self):
        """
        Downloads all the docs if the remote version is different than the local
        version.
        """
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
                os.remove(path)
        
    def updateDoc(self, path):
        """
        sends doc information to the corresponding doc on Google Docs.
        """
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
    """
    Houses all methods that respond to specific pyinotify events.
    """
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
    """
    Daemon object with basic initialization.
    """
    def __init__(self):
        self.stdin   = '/dev/null'
        self.stdout  = '/tmp/sync.log'
        self.stderr  = '/tmp/sync.log'
        self.pidfile = '/tmp/sync.pid'
        
        self.sync = DocSync()
    
    def run(self):
        self.sync.start()

if __name__ == "__main__":
    daemon = SyncDaemon()
    parser = argparse.ArgumentParser()
    
    parser.add_argument('command',
                        action='store',
                        choices=['start','stop','restart','debug','pull'],
                        help='What to do. Use debug to start in the foreground')
    
    args = parser.parse_args()
    
    # Execute the command
    if 'start' == args.command:
        daemon.sync.authorize()
        daemon.start()
    elif 'stop' == args.command:
        daemon.stop()
        print 'stopped'
    elif 'restart' == args.command:
        daemon.restart()
    elif 'debug' == args.command:
        daemon.sync.authorize()
        daemon.run()
    elif 'pull' == args.command: #do not run the daemon, but rather just download the documents
        daemon.sync.authorize()
        daemon.sync.getEverything()
    else:
        print "Unkown Command"
        sys.exit(2)
    sys.exit(0)

