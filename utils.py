from xmpp import *
import sys, random, time, base64, urllib3, smtplib
from email.mime.text import MIMEText
from configparser import ConfigParser
from threading import Timer, Semaphore
from jabber_rpc import Parser
from random import uniform
import traceback

class Notifier:
    """ Notification object """
    def __init__(self):
        self.email = ''
        self.phone = '00000000000'
        
    """ Send an email notification with message to a certain recipient. 
    If a recipient isn't specified, send to the admin email defined in the config file """
    def send_email(self, message, recipient=None):
        if recipient == None:
            recipient = self.email
        s = smtplib.SMTP('localhost')
        s.sendmail('alerts@orochi.quae.co.uk', [recipient], message)
        s.quit()
    
    """ Using the Esendex REST API, send an SMS message with provided content to a given phone number.
    If the phone number isn't specified, the SMS will be sent to the administrator number defined in the system config file"""
    def send_sms(self, message):
        xml  = '''<?xml version=\"1.0\" encoding=\"UTF-8\"?><message><from>''' + self.phone + '''</from><to>''' + self.phone + '''</to><type>SMS</type><body>''' + message + '''</body><validity>0</validity></message>'''
        #print(xml)
        
        username = ''
        password = ''
        
        encoded_details = base64.encodestring('%s:%s' % (username, password))[:-1]
        auth = "Basic %s" % encoded_details
        
        url = ''
        
        headers = {'Authorization' : auth, 'Content-Type' : 'text/xml'}
        
        request = urllib3.Request(url, xml, headers)
        try:
            handle = urllib3.urlopen(request)
        except IOError as e:
            if hasattr(e, 'reason'):
                print('Reason: '), e.reason
                return e
            elif hasattr(e, 'code'):
                print('Error code:'), e.code
                return e
        else:
            response = handle.read()
            #print(response )
        
        return True
        
class MessageScheduler:
    """ Message scheduler and queue object """
    def __init__(self, handler):
        self.handler = handler
        self.messages = {}
        self.sem = Semaphore()
        self.parser = Parser()
        
    """ Check if a message is managed by the Scheduler by ID """
    def is_managed(self, iq_id):
        try:
            if self.messages[iq_id] != None:
                return True
        except KeyError:
            pass
        return False
        
    """ Shutdown the queue, remove all queued messages and cleanup """
    def end(self):
        self.sem.acquire()
        while len(self.messages) > 0:
            key, (message, action) = self.messages.popitem()
            message.cancel()
        del self.messages
        self.sem.release()
        
    """ Add a message to the queue with optional random offset and method to perform when a result is recieved.
    Immediately sends a message, and schedules another retry message to be sent ten seconds later. """
    def add_message(self, message, action=None, offset=False):
        if offset == True:
            delay = uniform(0, 60)
        else:
            delay = 0
            
        # Send initial message with no, or offset amount of delay
        try:
            message_id = int(time.time() * 100)
            message.setAttr('id', message_id)
#            self.sem.acquire()
            # Setup a second message to be sent after timeout
            retry_job = Timer(delay + 10, self.handler, (message, True))
            self.messages[message_id] = retry_job, action
            retry_job.start()
#            self.sem.release()
            Timer(delay, self.handler, (message,)).start()
        except AttributeError:
            print("Can't send message, queue is shutting down.")
#        self.sem.release()
        
    """ Passed an iq node, removes any future scheduled message retries and if applicable, executes the assigned method. """
    def received_response(self, iq_node):
#        print('Message response received!')
        # Cancel subsequent message when response receieved
        self.sem.acquire()
        iq_id = iq_node.getAttr('id')
        sender = iq_node.getFrom()
        # If this is a response to the retry, and the original message got a response after the retry was sent
        try:
            thread, action = self.messages.pop(int(iq_id))
            try:
                thread.cancel()
            except:
                print('Error removing retry message')
            if action != None:
                try:
                    apply(action, [sender, iq_node.getTag('query')])
                except:
                    print('Failed to execute handler')
#                    print(sys.exc_info())
                    traceback.print_exc()
        except KeyError:
            print('Response already receieved')
        # No way to tell whether a response is to a retry or original. If original, then successful response, so cancel retry
        self.sem.release()
    
class Configuration:
    """ System configuration object, takes configuration filename as argument """
    def __init__(self, filename='config.cfg'):
        self.config = ConfigParser()
        self.config.read(filename)

    """ Returns the configuration details for the section of the config file concerning the database """
    def get_db_details(self):
        server = self.config.get('database', 'server')
        database = self.config.get('database', 'database')
        username = self.config.get('database', 'username')
        password = self.config.get('database', 'password')
        return server, database, username, password
#        return self.config.get('database', 'server'), self.config.get('database', 'username'), self.config.get('database', 'username'), 

    def get_conn_debug(self):
        return self.config.get('connection', 'debug')
        
    def get_db_server(self):
        return self.config.get('database', 'server')

    def get_db_database(self):
        return self.config.get('database', 'database')

    def get_db_username(self):
        return self.config.get('database', 'username')

    def get_db_password(self):
        return self.config.get('database', 'password')
        
class ConnectionException(Exception):
    def __init__(self, message):
        self.message = message
        
class ErrorResult(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return repr(self.message)
    
class Connection:
    """ Connection class, establish connection to XMPP """
    def __init__(self, entity, password='', resource='skynet', static=False):
        self.server = ''
        self.muc_entity = ''
        self.resource = resource
        config = Configuration()

        if static == True:
            self.entity = entity
        else:
            rand_id = random.randint(1, 2000)
            self.entity = entity + '-' + str(rand_id)
        # Connection
        if config.get_conn_debug() == 'True':
            self.conn = Client(self.server,5222)
        else:
            self.conn = Client(self.server,5222,debug=[])
                            
        self.conres = self.conn.connect()
        if not self.conres:
            print("Unable to connect to server %s!")%self.server
            sys.exit(1)
        if self.conres != 'tls':
            print("Warning: unable to estabilish secure connection - TLS failed!")
        if static != True:
        # Registration
            self.regres = features.register(self.conn, self.server, {'username':self.entity, 'password':password})
            if not self.regres:
                print("Unable to register on %s.") %self.server
                sys.exit(1)
            else:
                print(self.regres)

        # Authentication
        self.authres = self.conn.auth(self.entity, password, self.resource)
        if not self.authres:
            print("Unable to authorize on %s - check login/password.")%self.server
            sys.exit(1)
        if self.authres != 'sasl':
            print("Warning: unable to perform SASL auth os %s. Old authentication method used!")%self.server
        self.conn.sendInitPresence()

    def unregister(self):
        features.unregister(self.conn, self.server)
    
    """ Connect, sync message and unregister """
    def send_single_message(self, message):
        result = self.conn.SendAndWaitForResponse(message)
        self.unregister()
        if result.getAttr('type') == 'error':
            raise ErrorResult('Service returned an error')
        return result
    
    """ Connect, sync message """
    def send_message(self, message):
        result = self.conn.SendAndWaitForResponse(message)
        if result.getAttr('type') == 'error':
            raise ErrorResult('Service returned an error')
        return result

    def get_entity_name(self):
        return self.entity, '@' + self.server + '/' + self.resource

    """ Join MUC by name """
    def join_muc(self, room_name):
        muc_jid = room_name + '@' + self.muc_entity
        message = Presence(muc_jid + '/' + self.entity)
        message.addChild('x', namespace=NS_MUC)
        self.conn.send(message)
        return muc_jid

    """ Return XMPPPy connection object """
    def get_conn(self):
        return self.conn
        
class Logging:
    
    """ Object for logging events, takes an XMPP connection as an argument and joins logging MUC """
    def __init__(self, conn):
        self.conn = conn.get_conn()
        self.log_muc = conn.join_muc('logging')
        
    """ Sends a message of type info to the logging MUC """
    def info(self, message):
        message = Message(self.log_muc,'[INFO] %s' % message)
        message.setType('groupchat')
        self.conn.send(message)
    
    """ Sends a message of type error to the logging MUC """
    def error(self, message):
        message = Message(self.log_muc,'[ERROR] %s' % message)
        message.setType('groupchat')
        self.conn.send(message)
    
#      Database(settings.get_database_server())
