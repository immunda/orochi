from sqlalchemy import *
from xmpp import *
from time import sleep
from utils import Connection, MessageScheduler, Logging
from jabber_rpc import Parser
from database import Database
from Queue import Queue
from types import *
from threading import Thread
import sys
from datetime import datetime

import traceback
    
class Controller:
    
    """ Controller object. Initializes various data structures used by object. Establishes connection with XMPP server,
    connects to poller, aggregator and logging Multi-User Chats and registers stanza handlers. """
    def __init__(self):

        self.db = Database()

        entity_prefix = 'controller'

        conn = Connection(entity_prefix, static=True)
        self.entity_name, self.entity_suffix = conn.get_entity_name()
        
        # List of nodes known to the controller
        self.poller_map = {}
        self.poller_pool = {}
        
        self.job_map = {}
        self.job_pool = []

        # Message scheduler
        self.sched = MessageScheduler(self.message_handler)
        
        self.log = Logging(conn)
        conn.join_muc('pollers')
        conn.join_muc('aggregators')

        self.parser = Parser()
        
        self.establish_jobs()
        
        self.conn = conn.get_conn()

        self.conn.RegisterHandler('iq',self.result_handler,'result')
        self.conn.RegisterHandler('iq',self.set_handler,'set')
        self.conn.RegisterHandler('presence',self.presence_handler)

        self.go_on()
        
    """ Called by the presence handler when an entity connects to the aggregator or poller MUCs.
    Used to inspect retreive service information from an entity, required in the XEP Jabber-RPC standard. """
    def disco_lookup(self, recipient):
        self.log.info('Performing discovery lookup.')
        message = Iq('get', queryNS=NS_DISCO_INFO, to=recipient)
        self.sched.add_message(message, self.disco_handler)
        
    """ Method passed and used as handler for messages by MessageScheduler.
    Sends messages and logs an error if message send is a retry """
    # Handler used by message scheduling class
    def message_handler(self, message, retry=False):
#        print 'Sending message.'
        if retry == True:
            self.log.error('Message timed out, resending.')
        self.conn.send(message)
#
#   MESSAGE HANDLERS
#
    """ Handler for presence stanzas recieved by the XMPP listener.
    If 
    """
    def presence_handler(self, conn, presence_node):
        sender = presence_node.getFrom()
        presence_type = presence_node.getAttr('type')
        # Ignore self and presence announcements from logging MUC
        if sender.getResource() != 'controller':
            if presence_type == 'unavailable':
                if sender.getNode() == 'aggregators' or sender.getNode() == 'pollers':
                    self.remove_entity(sender.getNode(), sender)
            elif sender.getNode() == 'aggregators' or sender.getNode() == 'pollers':
                # Check the service discovery details for a connecting node.
                self.disco_lookup(sender)
        raise NodeProcessed
        
    """ IQ set handler, runs RPC methods in whitelist """
    def set_handler(self, conn, iq_node):
        query_node = iq_node.getQueryChildren()
        for node in query_node:
            try:
                method = node.getTagData('methodName')
                method_whitelist = ['get_group', 'get_groups', 'create_group', 'update_group', 'remove_group',
                'get_monitor', 'get_monitors', 'create_monitor', 'update_monitor', 'remove_monitor', 'get_monitors_by_gid',
                'get_job', 'get_jobs', 'create_job', 'update_job', 'remove_job',
                'get_evaluation', 'get_evaluations', 'create_evaluation', 'update_evaluation', 'remove_evaluation',
                'get_results', 'get_results_day', 'get_results_week', 'get_results_hour',
                'poller_failure',
                'get_aggregator']
                if method in method_whitelist:
                    method = getattr(self, method)
                    try:
                        try:
                            params = node.getTag('params').getChildren()
                            args = self.parser.get_args(params, iq_node.getFrom())
                        except AttributeError:
                            args = []
                            
                        status, parameters  = apply(method, args)
                        message = self.parser.rpc_response(iq_node.getFrom(), iq_node.getID(), status, parameters)
                        self.conn.send(message)
                    except TypeError:
#                        print sys.exc_info()
                        conn.send(iq_node.buildReply('error'))
                else:
                    conn.send(iq_node.buildReply('error'))
                    self.log.error('Method not in whitelist')
            except AttributeError:
                traceback.print_exc()
                conn.send(iq_node.buildReply('error'))
        raise NodeProcessed

    def result_handler(self, conn, iq_node):
        # Check if the reponse is managed by scheduler
        if self.sched.is_managed(int(iq_node.getAttr('id'))):
            self.sched.received_response(iq_node)
        raise NodeProcessed
#   END MESSAGE HANDLERS

#
# BEGIN SCHEDULER RESPONSE HANDLERS
#
    def disco_handler(self, sender, query_node):
        if query_node.getNamespace() == NS_DISCO_INFO:
            entity_type = query_node.getTagAttr('identity', 'type')
            if entity_type == 'aggregator' or entity_type == 'poller':
                adjusted_jid = JID(sender.getResource() + '@quae.co.uk/skynet')
                category = query_node.getTagAttr('identity', 'category')
                self.log.info('Registering node %s' % adjusted_jid)
                self.add_entity(entity_type, category, adjusted_jid)
        else:
            self.log.error('Receieved iq message with incorrect namespace')

    def assign_job(self, sender, query_node):
        if query_node.getNamespace() == NS_RPC:
            poller, job_id = self.parser.get_args_no_sender(query_node.getTag('methodResponse').getTag('params').getChildren())
            poller_jid = JID(poller)
            job_id = int(job_id)
            job = None
            for i in range(len(self.job_pool)):
                if self.job_pool[i]['id'] == job_id:
                    job = self.job_pool.pop(i)
                    self.log.info('Removing job %s from the job pool' % job_id)
                    break
            if job != None:
                self.job_map[poller_jid].append(job)
                self.log.info('Job %s successfully assigned to %s' % (job_id, poller_jid))
        else:
            self.log.error('Receieved iq message with incorrect namespace')
            
    def poller_removed(self, sender, query_node):
        if query_node.getNamespace() == NS_RPC:
            args = self.parser.get_args_no_sender(query_node.getTag('methodResponse').getTag('params').getChildren())
            adjusted_jid = JID(args[0])
            unassigned_jobs = self.job_map.pop(adjusted_jid)
            for job in unassigned_jobs:
                self.log.info('Adding job %s to the job pool' % job['id'])
                self.job_pool.append(job)
            parent_poller = None
            for aggregator, pollers in self.poller_map.items():
                for poller, segment in pollers:
                    if poller == adjusted_jid:
                        parent_aggregator = aggregator
                        pollers.remove((poller, segment))
                        break
            self.log.info('Removed %s from %s' % (adjusted_jid, parent_aggregator))
            self.assign_pooled_jobs()            
        else:
            self.log.error('Receieved iq message with incorrect namespace')
# END SCHEDULER HANDLERS

#
#   BEGIN RPC METHODS
#
# Requested by an aggregator when an assigned poller has failed/disconnected.
    def poller_failure(self, sender, previous_poller):
        pollers = self.poller_map[JID(sender)]
        poller_jid = JID('pollers@conference.quae.co.uk/' + JID(previous_poller).getNode())
        try:
            pollers.remove(poller_jid)
            message = 'Removed failed poller'
            try:
                self.rebalance_pollers()
            except:
                print sys.exc_info()
            return 'success', [message]
        except:
            return 'failure', ['Failed to remove poller']

# Group operations
    def get_group(self, sender, name):
        group = self.db.get_group_by_name(name)
        if group != None:
            return 'success', [group]
        return 'failure', ['No such group exists']
    
    def get_groups(self, sender):
        groups = self.db.get_groups()
        if groups != None:
            return 'success', [groups]
        return 'failure', ['Failed to retreieve groups']

    def create_group(self, sender, name, desc):
        existing = self.db.get_group_by_name(name)
        if existing == None:
            self.db.create_group(name, desc)
            return 'success', ['Sucessfully created group %s' % name]
        return 'failure', ['failure']
    
    def update_group(self, sender, id, name, desc):
        group = self.db.get_group_by_id(id)
        if group != None:
            self.db.update_group(id, name, desc)
            return 'success', ['Successfully update group %s' % name]
        return 'failure', ['Failed to update group']
        
    def remove_group(self, sender, name):
        self.db.remove_group_by_name(name)
        if self.db.get_group_by_name(name) == None:
            return 'success', ['Successfully remove group %s' % name]
        else:
            return 'failure', ['Failed to remove group %s' % name]
# Monitor operations

    def get_monitor(self, sender, name):
        try:
            monitor = self.db.get_monitor(name)
            if monitor != False:
                return 'success', [monitor]
        except TypeError:
            return 'failure', ['No such monitor exists']
        return 'failure', ['No such monitor exists']
        
    def get_monitors(self, sender, group=None):
        try:
            if group != None:
                monitors = self.db.get_monitors(group)
            else:
                monitors = self.db.get_monitors()
            return 'success', [monitors]
        except AttributeError:
            return 'failure', ['Failed to retrieve monitors']
            
    def get_monitors_by_gid(self, sender, group_id):
        try:
            if group_id != None:
                monitors = self.db.get_monitors_by_gid(group_id)
                return 'success', [monitors]
        except AttributeError:
            pass
        return 'failure', ['Failed to retrieve monitors']
    
    def create_monitor(self, sender, name, description, group):
        if self.db.create_monitor(name, description, group) == True:
            return 'success', ['Successfully create monitor %s' % name]
        return 'failure', ['Failed to create monitor']
        
    def update_monitor(self, sender, name, description, group):
        group = self.db.get_group_by_id(id)
        if group != None:
            self.db.update_group(id, name, desc)
            return 'success', ['Successfully update monitor %s' % name]
        return 'failure', ['Failed to update monitor']
        
    def remove_monitor(self, sender, name):
        self.db.remove_monitor_by_name(name)
        if self.db.get_monitor_by_name(name) == None:
            return 'success', ['Successfully removed monitor %s' % name]
        else:
            return 'failure', ['Failed to remove monitor %s' % name]
        
# job operations

    def get_job(self, sender, mon, id):
        try:
            job = self.db.get_job(id, mon)
            return 'success', [job]
        except TypeError:
            return 'failure', ['No such job exists']
            
    def get_jobs(self, sender, mon):
        try:
            jobs = self.db.get_jobs(mon)
            return 'success', [jobs]
        except AttributeError:
            return 'failure', ['Failed to retreieve jobs']

    def create_job(self, sender, mon, address, protocol, frequency, interface, resource):
        if self.db.get_monitor(mon) != None:
            if self.db.create_job(address, protocol, frequency, interface, resource, mon) == True:
                return 'success', ['Successfully created a job for %s' % mon]
        return 'failure', ['Failed to create job']

    def update_job(self, sender, mon, id, address, protocol, frequency, interface, resource):
        existing = self.db.get_job(id, mon)
        if existing != None:
            self.db.update_job(id, address, protocol, frequency, interface, resource)
            return 'success', ['Successfully updated job']
        else:
            return 'failure', ['failure']
            
    def remove_job(self, sender, mon, id):
        self.db.remove_job(id)
        if self.db.get_job(id) == None:
            return 'success', ['Successfully removed job']
        else:
            return 'failure', ['failure']
        
    # Result read operations
    
    def get_results(self, sender, monitor, job, start_datetime, end_datetime):
        results = self.db.get_results(monitor, job, start_datetime, end_datetime)
        if results == None:
            return 'failure', ['No such results exist']
        elif results != False:
            return 'success', results
        return 'failure', ['Failed to retreive specificied results']
        
    def get_results_day(self, sender, monitor, job, start_datetime):
        results = self.db.get_results_day(monitor, job, start_datetime)
        if results == []:
            return 'failure', ['No such results exist']
        elif results != False:
            job_details = self.db.get_job(job, monitor)
            return 'success', [job_details, results]
        return 'failure', ['Failed to retreive specificied results']
        
    def get_results_week(self, sender, monitor, job, start_datetime):
        pass
    
    def get_results_hour(self, sender, monitor, job, start_datetime):
        results = self.db.get_results_hour(monitor, job, start_datetime)
        if results == []:
            return 'failure', ['No such results exist']
        elif results != False:
            job_details = self.db.get_job(job, monitor)
            return 'success', [job_details, results]
        return 'failure', ['Failed to retreive specificied results']
#   END RPC METHODS

#
#   BEGIN PRIVATE METHODS
#
    """ Called on successful DISCO request.
    Will register Poller or Aggregator, send jobs or balance jobs. """
    def add_entity(self, entity_type, segment, entity):
        if entity_type == 'aggregator':
            self.poller_map[entity] = []
            # If pollers have been added, but there were no aggregators running
            self.assign_pooled_pollers()
            if len(self.poller_map) > 1:
                self.rebalance_pollers()
            self.assign_pooled_jobs()
                
        elif entity_type == 'poller':
            self.job_map[entity] = []
            
            self.poller_pool[entity] = segment
            self.assign_pooled_pollers()
            
            if len(self.poller_map) > 1:
                self.rebalance_pollers()
            
            self.assign_pooled_jobs()
            
            if len(self.job_map) > 1:
                self.rebalance_jobs()
            # Give poller to appropriate aggregator
            print 'Added %s to %ss' % (entity, entity_type)
        self.log.info('Node %s was successfully registered with the controller' % entity)

    """ Removing Poller or Aggregator """
    def remove_entity(self, entity_type, entity):
        try:
            if entity_type == 'aggregators':
                adjusted_jid = JID(JID(entity).getResource() + '@quae.co.uk/skynet')
                unassigned_pollers = self.poller_map.pop(adjusted_jid)
                for poller, segment in unassigned_pollers:
                    self.poller_pool[poller] = segment
                self.log.info('Removed %s' % adjusted_jid)
                if len(self.poller_pool) > 0:
                    # Try and assign pooled pollers
                    if not self.assign_pooled_pollers():
                        for poller, segment in unassigned_pollers:
                            message = self.parser.rpc_call(poller, 'aggregator_failure', [])
                            self.sched.add_message(message)
                    
            elif entity_type == 'pollers':
                adjusted_jid = JID(JID(entity).getResource() + '@quae.co.uk/skynet')
                if len(self.poller_map) > 0:
                    parent_aggregator = None
                    for aggregator, pollers in self.poller_map.items():
                        for poller, segment in pollers:
                            if adjusted_jid == poller:
                                parent_aggregator = aggregator
                                break
                    if parent_aggregator != None:
                        remove_call = self.parser.rpc_call(parent_aggregator, 'remove_poller', [str(adjusted_jid)])
                        self.sched.add_message(remove_call, self.poller_removed)
                else:
                    try:
                        self.job_map.pop(adjusted_jid)
                        self.poller_pool.pop(adjusted_jid)
                        self.log.info('Poller not assigned, sucessfully removed')
                    except:
                        self.log.error('Failed to remove poller')
                        traceback.print_exc()
        except ValueError:
            self.log.error('Failed to remove %s' % entity)

    """ Assign unassigned pollers """
    def assign_pooled_pollers(self):
        if len(self.poller_map) > 0:
            while len(self.poller_pool) > 0:
                unassigned_poller, segment = self.poller_pool.popitem()
                chosen_aggregator = None
                poller_comp = None
                for aggregator, pollers in self.poller_map.items():
                    # If first loop or number of pollers assigned to aggregator is less than comp, make this agg the comp
                    if (chosen_aggregator == None and poller_comp == None) or len(pollers) < poller_comp:
                        chosen_aggregator = aggregator
                        poller_comp = len(pollers)
                if chosen_aggregator != None:
                    # Assign Poller to the Aggregtor with least assigned Pollers
                    
                    message = self.parser.rpc_call(chosen_aggregator, 'add_poller', [str(unassigned_poller)])
                    self.sched.add_message(message)
                    
                    for job in self.job_map[unassigned_poller]:
                        message = self.parser.rpc_call(chosen_aggregator, 'move_job', [str(unassigned_poller), job['id'], job['address'], job['protocol'], job['frequency'], job['interface'], job['resource'], job['segment']])
                        self.sched.add_message(message)
                    self.poller_map[chosen_aggregator].append((unassigned_poller, segment))
            return True
        else:
            self.log.info('No aggregators available for poller assignment')
            return False
    
    """ Get Pollers for a given network segment """
    def get_segment_pollers(self, segment):
        segment_pollers = {}
        for aggregator, pollers in self.poller_map.items():
            for poller, poller_segment in pollers:
                if poller_segment == segment:
                    segment_pollers[poller] = self.job_map[poller]
        return segment_pollers
        
    """ Called to allocate unassigned jobs """
    def assign_pooled_jobs(self):
        if len(self.job_map) > 0 and len(self.poller_map) > 0:
            for job in self.job_pool:
                unassigned_job = job
                least_loaded = None
                job_comp = None
                pollers = self.get_segment_pollers(unassigned_job['segment'])
                for poller, jobs in pollers.items():
#                for poller, jobs in self.job_map.items():
                    if (least_loaded == None and job_comp == None) or len(jobs) < job_comp:
                        least_loaded = poller
                        job_comp = len(jobs)
                if least_loaded != None:
                    chosen_aggregator = None
                    for aggregator, pollers in self.poller_map.items():
                        for poller, segment in pollers:
                            if poller == least_loaded:
                                chosen_aggregator = aggregator
                                break
                    if chosen_aggregator != None:
                        self.send_job(unassigned_job, least_loaded, chosen_aggregator)
        else:
            self.log.info('No assigned pollers available for job assignment')
        #print 'Job map %s' % self.job_map
        #print 'Job pool %s' % self.job_pool
    
    """ Rebalance pollers, compares amount assigned to each Aggregator, and moves across to
    another Poller if there's at least 2 more than another Aggregator """
    def rebalance_pollers(self):
        self.log.info('Attempting to rebalance pollers')

        poller_comp = None
        least_pollers = None
        most_pollers = None
        # Retrieve aggregators with least and most pollers
        
        for aggregator, pollers in self.poller_map.items():
            if poller_comp == None:
                least_pollers = aggregator
                most_pollers = aggregator
            elif len(pollers) < poller_comp:
                least_pollers = aggregator
            elif len(pollers) > poller_comp:
                most_pollers = aggregator
            poller_comp = len(pollers)
        
        if least_pollers != None and most_pollers != None:
            # If the difference between the two pollers is worth balancing
            if (len(self.poller_map[most_pollers]) - len(self.poller_map[least_pollers])) > 1:
                poller, segment = self.poller_map[most_pollers].pop()
                self.poller_map[least_pollers].append((poller, segment))
                self.sched.add_message(self.parser.rpc_call(least_pollers, 'add_poller', [str(poller)]))
                self.sched.add_message(self.parser.rpc_call(most_pollers, 'remove_poller', [str(poller)]))
                self.rebalance_pollers()
            else:
                self.log.info('Pollers balanced')
                return True
               
    """ Similar to above, checks number of assigned jobs through the system, and will level them across all Pollers """ 
    def rebalance_jobs(self):
        self.log.info('Attempting to rebalance jobs')
        job_comp = None
        least_jobs = None
        most_jobs = None
        
        network_segment = 'skynet'
        pollers = self.get_segment_pollers(network_segment)
        
#        for poller, jobs in self.job_map.items():
        for poller, jobs in pollers.items():
            print poller
            if job_comp == None:
                least_jobs = poller
                most_jobs = poller
            elif len(jobs) < job_comp:
                least_jobs = poller
            elif len(jobs) > job_comp:
                most_jobs = poller
            job_comp = len(jobs)
            
        if least_jobs != None and most_jobs != None:
            if (len(self.job_map[most_jobs]) - len(self.job_map[least_jobs])) > 1:
                job = self.job_map[most_jobs].pop()
                self.job_map[least_jobs].append(job)
                least_parent = None
                most_parent = None
                for aggregator, pollers in self.poller_map.items():
                    for poller, node_segment in pollers:
                        if network_segment == node_segment:
                            if poller == least_jobs:
                                least_parent = aggregator
                            if poller == most_jobs:
                                most_parent = aggregator
                            if least_parent != None and most_parent != None:
                                break
                    
                if least_parent != None and most_parent != None:
                    self.log.info('Moving job %s to %s' % (job['id'], least_jobs))
                    self.sched.add_message(self.parser.rpc_call(most_parent, 'remove_job', [job['id']]))
                    self.sched.add_message(self.parser.rpc_call(least_parent, 'run_job', [str(least_jobs), job['id'], job['address'], job['protocol'], job['frequency'], job['interface'], job['resource']]), offset=True)
                self.rebalance_jobs()
            else:
                self.log.info('Jobs balanced')
                return True
                
    """ Retrieve jobs on startup """    
    def establish_jobs(self):
        monitors = self.db.get_monitors()
        self.log.info('Retrieving jobs')
        for monitor in monitors:
            jobs = self.db.get_jobs(monitor.name)
            for job in jobs:
                job = dict(job)
                segment_name = self.db.get_segment_name(job['segment'])
                job['segment'] = segment_name
                # Make the poll freq stored every minute minimum
                job['frequency'] = job['frequency'] * 60
                self.job_pool.append(job)
        self.log.info('%s jobs added to the pool' % len(self.job_pool))
        
    """ Send job to Aggregator to forward to Poller """
    def send_job(self, job, poller, aggregator):
        message = self.parser.rpc_call(aggregator, 'run_job', [str(poller), job['id'], job['address'], job['protocol'], job['frequency'], job['interface'], job['resource']])
        self.log.info('Sending job %s to %s' % (job['id'], aggregator))
        self.sched.add_message(message, self.assign_job, offset=True)
        
    def step_on(self):
        try:
            self.conn.Process(1)
        except KeyboardInterrupt: return 0
        return 1

    def go_on(self):
        while self.step_on(): pass

# If file called directly...
if __name__ == '__main__':

    controller = Controller()
