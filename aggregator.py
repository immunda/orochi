#!/usr/bin/python
from xmpp import *
from sqlalchemy import *
import time, sys, random, traceback
from utils import Connection, Configuration, MessageScheduler, Notifier, Logging
from database import Database
from jabber_rpc import Parser

class Aggregator:
    """ Aggregator object 
    Establishes connection and joins MUCs. Registers handlers
    """
    def __init__(self):
        config = Configuration()

        self.db = Database()
        self.results = self.db.get_table('results')
        
        self.job_map = {}
        self.job_pool = []
        
        self.failed_jobs = []
        
        self.evals = {}
        
        self.notifier = Notifier()
        
        self.sched = MessageScheduler(self.message_handler)

        conn = Connection('aggregator', 'roflcake')
        entity_prefix, entity_suffix = conn.get_entity_name()
        self.entity_name = entity_prefix + entity_suffix

        self.log = Logging(conn)
        conn.join_muc('aggregators')
        
        self.conn = conn.get_conn()
        
        self.roster = self.conn.getRoster()
        
        self.conn.RegisterHandler('iq',self.set_handler,'set')
        self.conn.RegisterHandler('iq',self.get_handler,'get')
        self.conn.RegisterHandler('iq',self.result_handler,'result')
        self.conn.RegisterHandler('presence',self.presence_handler)
        
        self.temp_messages = []
        
        self.parser = Parser()

        self.go_on()

    """ Handler for scheduler """
    def message_handler(self, message, retry=False):
        #print 'Sending message.'
        if retry == True:
            self.log.error('Timed out, attempting to resend.')
        self.conn.send(message)
    
# HANDLERS
    """ IQ result handler """
    def result_handler(self, conn, iq_node):
        if self.sched.is_managed(int(iq_node.getID())):
            self.sched.received_response(iq_node)
        raise NodeProcessed
            
    """ Presence handler """
    def presence_handler(self, conn, presence_node):
        if len(self.job_map) > 0:
            sender = presence_node.getFrom()
            if presence_node.getAttr('type') == 'unavailable':
                failed_poller = None
                for poller in self.job_map:
                    if poller == sender:
                        failed_poller = poller
                        break
                        
                if failed_poller != None:
                # Only used if Controller has gone offline
                    self.log.info('Poller %s has gone offline.' % failed_poller)
        raise NodeProcessed
                
    """ IQ get handler """
    def get_handler(self, conn, iq_node):
        if iq_node.getQueryNS() == NS_DISCO_INFO:
            reply = iq_node.buildReply('result')
            identity = Node('identity', {'category':'skynet', 'type':'aggregator'})
            reply.setQueryPayload([identity])
            conn.send(reply)
        raise NodeProcessed

    """ IQ set handler. Permits RPC calls in the whitelist, else returns error message """
    def set_handler(self, conn, iq_node):
        sender = iq_node.getFrom()
        iq_id = iq_node.getAttr('id')
        
        if sender == 'controller@quae.co.uk/skynet':
            query_node = iq_node.getQueryChildren()
            for node in query_node:
                try:
                    method = node.getTagData('methodName')
                    method_whitelist = ['run_job', 'add_poller', 'remove_poller', 'remove_job', 'move_job']
                    if method in method_whitelist:
                        method = getattr(self, method)
                        try:
                            try:
                                params = node.getTag('params').getChildren()
                                args = self.parser.get_args(params)
                            except AttributeError:
                                args = []
                            status, parameters = apply(method, args)
                            message = self.parser.rpc_response(iq_node.getFrom(), iq_node.getID(), status, parameters)
                            conn.send(message)
                        except TypeError:
                            #print sys.exc_info()
                            conn.send(iq_node.buildReply('error'))
                    else:
                        conn.send(iq_node.buildReply('error'))
                        self.log.error('Method called not in whitelist')
                except AttributeError:
                    #print sys.exc_info()
                    conn.send(iq_node.buildReply('error'))

        if len(self.job_map) > 0:
            if sender in self.job_map:
                query_node = iq_node.getQueryChildren()
                for node in query_node:
                    try:
                       method = node.getTagData('methodName')
                       method_whitelist = ['add_result']
                       if method in method_whitelist:
                           method = getattr(self, method)
                           try:
                               try:
                                   params = node.getTag('params').getChildren()
                                   args = self.parser.get_args(params)
                               except AttributeError:
                                   args = []
                                   
                               status, parameters = apply(method, args)
                               message = self.parser.rpc_response(iq_node.getFrom(), iq_node.getID(), status, parameters)
                               conn.send(message)
                           except TypeError:
                               #print sys.exc_info()
                               conn.send(iq_node.buildReply('error'))
                       else:
                           conn.send(iq_node.buildReply('error'))
                    except AttributeError:
                        #print sys.exc_info()
                        conn.send(iq_node.buildReply('error'))
        
        raise NodeProcessed  # This stanza is fully processed
        
    """ Establish evaluations """
    def set_evals(self, job, evaluations):
        for evaluation in evaluations:
            if evaluation.string != None:
                self.evals[job].append((evaluation.comparison, str(evaluation.string)))
            elif evaluation.float != None:
                self.evals[job].append((evaluation.comparison, float(evaluation.float)))
            elif evaluation.int != None:
                self.evals[job].append((evaluation.comparison, int(evaluation.int)))
            else:
                self.log.info('Evaluation contains no comparison value.')
                
    """ Callback handler used by scheduler.add_message when poller replies on successful job assignment """
    def assign_job(self, sender, query_node):
        if query_node.getNamespace() == NS_RPC:
            params = query_node.getTag('methodResponse').getTag('params').getChildren()
            job_id = self.parser.get_args_no_sender(params)[0]
            job_id = int(job_id)
            self.job_map[JID(sender)].append(job_id)
            self.evals[job_id] = []
            
            evaluations = self.db.get_evaluations(job_id)
            self.set_evals(job_id, evaluations)
        else:
            pass
#            print 'Receieved iq message with incorrect namespace'
# END HANDLERS

# RPC METHODS
    """ Retains details of a job, allocates to Poller with least currently assinged jobs """
    def run_job(self, sender, poller, job, addr, proto, freq, dom, resource):
        # Checks a poller is assigned
        if len(self.job_map) > 0:
            # Determines which poller has least assigned jobs
#            job_comp = None
#            least_loaded = None
#            for poller, jobs in self.job_map.items():
#                num_jobs = len(jobs)
#                if job_comp != None:
#                    if len(jobs) < job_comp:
#                        least_loaded = poller
#                else:
#                    least_loaded = poller
#                job_comp = num_jobs
            
            message = self.parser.rpc_call(poller, 'run_job', [self.entity_name, job, addr, proto, freq, dom, resource])
            self.sched.add_message(message, self.assign_job)
            return 'success', [str(poller), int(job)]
        else:
            return 'failure', ['There are no pollers connected']
            
    """ Called when a job is moved to this Aggregator, sets up evals and details """
    def move_job(self, sender, poller, job, addr, proto, freq, dom, resource, segment):
        # Checks a poller is assigned
        if len(self.job_map) > 0:
            job_id = int(job)
            self.job_map[JID(poller)].append(job_id)
            self.evals[job_id] = []

            evaluations = self.db.get_evaluations(job_id)
            self.set_evals(job_id, evaluations)
            return 'success', ['Successfully moved job']
        else:
            return 'failure', ['There are no pollers connected']
    
    """ Removes job from the Aggregator, cleans up state """
    def remove_job(self, sender, job_id):
        job_id = int(job_id)
        parent_poller = None
        for poller, jobs in self.job_map.items():
            for i in range(len(jobs)):
                if jobs[i] == job_id:
                    jobs.pop(i)
                    parent_poller = poller
                    break
        try:
            self.evals.pop(job_id)
        except KeyError:
            pass
            
        if parent_poller != None:
            message = self.parser.rpc_call(parent_poller, 'remove_job', [job_id])
            self.sched.add_message(message)
            return 'success', ['Successfully removed job']
        else:
            return 'failure', ['Failed to remove job']
    
    """ Called by child Poller to deliver result """
    def add_result(self, sender, id, recorded, val):
        status = self.insert_result(id, recorded, val)
        if status != 'failure':
            return 'success', ['Sucessfully added result']
        else:
            messages = self.temp_messages
            self.temp_messages = []
            return 'failure', messages
        
    """ Called by Controller when Poller is assigned """
    def add_poller(self, sender, poller):
        # Subscribe to poller for presence updates
        poller_jid = JID(poller)
        self.roster.Subscribe(poller_jid)
        self.job_map[poller_jid] = []
        self.sched.add_message(self.parser.rpc_call(poller_jid, 'set_aggregator', [self.entity_name]))
        return 'success', ['Successfully added %s' % poller_jid]

    """ Called by Controller to remove references to Poller """
    def remove_poller(self, sender, poller):
        poller_jid = JID(poller)
        try:
            unassigned_jobs = self.job_map.pop(poller_jid)
            # If controller has also failed
#            for job in unassigned_jobs:
#                self.job_pool.append(job)
            self.roster.Unsubscribe(poller_jid)
            return 'success', [poller]
        except KeyError:
            return 'failure', ['Failed to remove poller %s' % poller]

# END RPC METHODS

    """ Used when inserting results supplied by add_result.
    Peforms evaluations, casts type, makes notifications and then stores into the database. """
    def insert_result(self, id, recorded, val, list_id=None):

        val_type = type(val).__name__
        
        try:
            evals = self.evals.get(int(id))
            if evals:
                for comparison, comp_val in evals:
                    if val_type == 'int' or val_type == 'float':
                        eval_statement = str(val) + str(comparison) + str(comp_val)
                    elif val_type == 'str':
                        eval_statement = str('\'' +  val + '\'') + str(comparison) + str('\'' + comp_val + '\'')
#                    print 'Eval statement: %s' % eval_statement
                    result = eval(eval_statement)
#                    print 'Eval result: %s' % result
                if result != True:
                    message = 'Job %s has caused an error! The value %s failed an evaluation.' % (id, comp_val)
                    self.log.error(message)
                    if id not in self.failed_jobs:
                        self.log.info('Sending notifications')
                        self.notifier.send_email(message)
                        self.notifier.send_sms(message)
                        self.failed_jobs.append(id)
                else:
                    if id in self.failed_jobs:
                        message = 'Job %s is back within normal parameters' % id
                        self.notifier.send_email(message)
                        self.log.info(message)
                        self.failed_jobs.remove(id)
                        
        except:
#            traceback.print_exc()
            self.temp_messages = ['Failed to evaluate returned result']
            return 'failure'
            
        if val_type == 'int':        
            if list_id != None:
                self.results.insert().execute(job=id, int=val, recorded=recorded, list=list_id)
            else:
                self.results.insert().execute(job=id, int=val, recorded=recorded)
        elif val_type == 'str':
            if list_id != None:
                self.results.insert().execute(job=id, string=val, recorded=recorded, list=list_id)
            else:
                self.results.insert().execute(job=id, string=val, recorded=recorded)
        elif val_type == 'float':
            if list_item != None:
                self.results.insert().execute(job=id, float=val, recorded=recorded, list=list_id)
            else:
                self.results.insert().execute(job=id, float=val, recorded=recorded)
        elif val_type == 'list':
            self.results.insert().execute(job=id, recorded=recorded, list=0)
            where = and_(self.results.c.recorded == recorded, self.results.c.list == 0)
            list_id = self.results.select(where).execute().fetchone().id
            #print "Retrieved list id %s" % list_id
            for element in val:
                self.insert_result(id, recorded, element, list_id)
        else:
            self.temp_messages = ['Unexpected data type receieved']
            return 'failure'
        
    """ Setup listener """
    def step_on(self):
        try:
            self.conn.Process(1)
        except KeyboardInterrupt:
            server = 'quae.co.uk'
            features.unregister(self.conn, server)
            #print 'Unregistered from %s' % server
            return 0
        return 1

    def go_on(self):
        while self.step_on(): pass
        
if __name__=='__main__':
    
    aggregator = Aggregator()
