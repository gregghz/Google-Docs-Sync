#!/usr/bin/env python

import gdata.docs.data
from gdata.docs.data import MIMETYPES
import gdata.docs.client
from gdata.gauth import ClientLoginToken
from gdata.client import Unauthorized

from signal import SIGTERM
from getpass import getpass
import sys, os, time, atexit, argparse

import pyinotify
import sqlite3
import urllib
from daemon import Daemon

import exceptions

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

class Folder(object):
    subfolders = []
    docs = []
    parent = None
    entry = None
    client = None

    home = os.path.expanduser('~')
    gdocs_folder = home +'/Google Docs'
    path = '/'

    def __init__(self, entry, parent=None, client=None):
        self.subfolders = []
        self.docs = []
        self.parent = parent
        self.entry = entry
        self.client = client
        
        # if no entry given, then assume this is the root
        if entry is None:
            feed = self.client.GetDocList(uri='/feeds/default/private/full/folder%3Aroot/contents/-/all')
        else:
            feed = self.client.GetDocList(uri=self.entry.content.src)
        
        # if it's not the root, set up the path properly
        if entry is not None and parent is not None:
            self.path = self.entry.title.text + '/'
            self.path = parent.path + self.path
            
        # make the paths exist on the filesystem
        # make sure to add self.gdocs_folder here
        if not os.path.isdir(self.gdocs_folder + self.path):
            os.mkdir(self.gdocs_folder + self.path, 0755)
            
        # run over each entry
        for e in feed.entry:
            is_folder = False
            is_doc = False
            
            # find out what folder this belongs to
            for link in e.link:
                if link.rel == 'http://schemas.google.com/docs/2007#parent':
                    #belongs_to = link.title
                    pass
        
            # use this loop to find out if we're in a doc or folder
            # @TODO: add PDFs and other types of files
            for cat in e.category:
                if cat.label == 'folder':
                    is_folder = True
                elif cat.label == 'document':
                    is_doc = True

            # add the folder or doc as appropriate
            if is_folder:
                self.add_folder(e)
            elif is_doc:
                self.add_doc(e)
        
    def __repr__(self):
        return self.path
        
    def add_folder(self, entry):
        self.subfolders.append(Folder(entry, self, self.client))
        
    def add_doc(self, entry):
        self.docs.append(Document(entry, self))
        
class Document(object):
    doc = None
    path = ''
    
    home = os.path.expanduser('~')
    gdocs_folder = home +'/Google Docs'
    
    db_file = gdocs_folder + '/.db'

    def __init__(self, doc, parent=None, client=None, force_download=False):
        self.doc = doc
        self.force = force_download
        self.parent = parent
        if self.parent is None:
            self.path = self.gdocs_folder + '/' + doc.title.text.replace('/', '-') + '.odt'
            self.client = client
        else:
            self.path = parent.gdocs_folder + parent.path + doc.title.text.replace('/', '-') + '.odt'
            self.client = parent.client
        self.db = DocDb()
        self.db.setDb(self.db_file)
        
        # make sure it exists on the file system
        self.save()
        
    def save(self):
        if self.doc.etag != self.db.getEtag(self.doc.resource_id.text) or self.force:
            print 'writing:', self.path
            self.client.Export(self.doc, self.path)
            self.db.addDoc(self.doc, self.path)
        else:
            # nothing to see here, move along
            pass
        
    def __repr__(self):
        return self.doc.title.text

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
    
    need_new_token = False
    
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
        try:
            self.getEverything()
            self._watchFolder()
        except Unauthorized, e:
            self.authorize()
            self.start()
        
        #TODO: make this work
        #self._setPeriodicSync()
        
        print 'stopped'
        
    def _watchFolder(self):
        """
        sets up the watching of the docs folder for changes.
        """
        wm = pyinotify.WatchManager()
        wm.add_watch(self.gdocs_folder, pyinotify.IN_MODIFY, rec=True)
        
        handler = EventHandler(self)
        notifier = pyinotify.Notifier(wm, handler)
        
        print 'waiting for changes . . .'
        notifier.loop()
        
    def authorize(self, username=None, password=None):
        """
        Make sure the user is authorized. Ask for username/password if needed.
        """
        self.db.setDb(self.db_file)
        
        token = self.db.getToken()
        if not token or self.need_new_token:
            if username is None:
                username = raw_input('Username: ')
            if password is None:
                password = getpass()

            self.client.ClientLogin(username, password, self.client.source)
            self.db.saveToken(self.client.auth_token.token_string)
            self.need_new_token = False
        else:
            # we still need to use username/password if they passed one in
            if username is not None or password is not None:
                if username is None:
                    username = raw_input('Username: ')
                if password is None:
                    password = getpass()
                self.client.ClientLogin(username, password, self.client.source)
                self.db.saveToken(self.client.auth_token.token_string)
            else:
                self.client.auth_token = ClientLoginToken(token)
            
        self.authd = True
            
    def getEverything(self, title=None, exact_match=False):
        """
        Downloads all the docs if the remote version is different than the local
        version.
        """
        try:
            if title is None:
                # build folder structure
                root_folder = Folder(None, None, self.client)
            else:
                exact = 'false'
                if exact_match:
                    exact = 'true'
                feed = self.client.GetDocList(uri='/feeds/default/private/full?title='+title+'&title-exact='+exact)
                # run over each entry
                for e in feed.entry:
                    is_doc = False
                
                    # use this loop to find out if we're in a doc or folder
                    # @TODO: add PDFs and other types of files
                    for cat in e.category:
                        if cat.label == 'document':
                            is_doc = True

                    # add the doc as appropriate
                    if is_doc:
                        Document(e, None, self.client, True)
        except Unauthorized, e:
            print 'token failed, getting new token . . .'
            self.need_new_token = True
            raise e
        
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
    parser = argparse.ArgumentParser(description='Manually or automatically sync your Google Docs')
                        
    parser.add_argument('-d', '--daemon',
                        action='store_true',
                        help='Start the sync in daemon mode (ignores all other options)')
    parser.add_argument('-s', '--stop-daemon',
                        action='store_true',
                        help='Stop the running daemon (ignores all other options)')
    parser.add_argument('-r', '--restart-daemon',
                        action='store_true',
                        help='Restart the running daemon (ignores all other options)')
    parser.add_argument('-p', '--pull',
                        action='store_true',
                        help='Download all documents from the server one time')
    parser.add_argument('-f', '--file',
                        action='store',
                        help='Download a single file by title')
    parser.add_argument('-x', '--exact',
                        action='store_true',
                        help='Always used with --file. Tells Watcher to force exact filename matches')
                        
    parser.add_argument('--username',
                        action='store',
                        default=None,
                        help='Set the username')
    parser.add_argument('--password',
                        action='store',
                        default=None,
                        help='Set the password')
    
    args = parser.parse_args()
    
    # Execute the command
    if args.daemon:
        daemon = SyncDaemon()
        daemon.sync.authorize(args.username, args.password)
        daemon.start()
    elif args.stop_daemon:
        daemon = SyncDaemon()
        daemon.stop()
        print 'stopped'
    elif args.restart_daemon:
        daemon = SyncDaemon()
        daemon.sync.authorize(args.username, args.password)
        daemon.restart()
    else: # the following is for non-daemon options. possibly more than one option at a time
        sync = DocSync() # need the non-daemon form for all of the following
        sync.authorize(args.username, args.password)
        if args.pull:
            sync.getEverything()
        elif args.file is not None:
            if args.exact:
                sync.getEverything(title=args.file, exact_match=True)
            else:
                sync.getEverything(title=args.file)
        else: # the default if no options are given
            sync.start()

    sys.exit(0)

