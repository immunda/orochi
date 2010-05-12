from pysnmp.entity.rfc3413.oneliner import cmdgen
from pysnmp.smi.error import NoSuchObjectError
from xmpp import *
import libvirt, sys
from os import system
from datetime import datetime
from time import time, sleep
from utils import Configuration, Connection, MessageScheduler, Logging
from jabber_rpc import Parser
from threading import Timer, Thread
from subprocess import Popen
import sched

class Job(Thread):
    """ Job object.
    Setup job details """
    def __init__(self, poller, id, address, protocol, frequency, domain, resource, conn, scheduler):
        self.poller = poller
        self.id = id
        self.address = address
        self.protocol = protocol
        self.frequency = frequency
        self.domain = domain
        self.resource = resource
        self.conn = conn
        self.scheduler = scheduler
        self.stop = False
        self.parser = Parser()
        self.cache = []
        Thread.__init__(self)
    
    """ Used to end the Job, sets stop boolean """
    def end(self):
        self.stop = True
    
    """ Sends messages stored in queue """
    def send_cached_results(self):
        for result in self.cache:
            message = self.parser.rpc_call(self.poller.get_aggregator(), 'add_result', result)
            self.scheduler.add_message(message, offset=True)
        
    """ Called by run method, executes polling method depending on Job protocol """
    def run_job(self):
#        self.add_job(self.conn)
        if self.protocol == 'snmp':
            messages = self.snmp_poll(self.resource, self.domain, self.address)
            if messages != None:
                for message in messages:
#                   print message
                    self.scheduler.add_message(message)
        elif self.protocol == 'test':
            result = self.test_execute(self.resource, self.address)
            if result != None:
                self.scheduler.add_message(result)

        elif self.protocol == 'libvirt':
            result = self.libvirt_poll(self.address, self.domain, self.resource)
            if result != None:
                self.scheduler.add_message(result)
        return True
        
    """ Run method, keep job scheduled by checking execution time """
    def run(self):
        added = round(time(), 3) - self.frequency
        while(self.stop == False):
            start = time()
            added += float(self.frequency)
            self.run_job()
            end = time() - added
            #print float(self.frequency) - end
            sleep(float(self.frequency) - end)

    """ Method called when script is executed.
    Exits before next poll if script doesn't complete """
    def test_execute(self, script, address):
        timeout = (time() + float(self.frequency)) - 1
        # Run script, redirect stderr and stdout to /dev/null
        subp = Popen('/bin/sh ' + script + '> /dev/null 2>&1', shell=True)
        while subp.poll() == None and time() < timeout:
            sleep(0.25)
            
        cur_time = float(time())
        aggregator = self.poller.get_aggregator()
        
        if cur_time >= timeout:
            subp.kill()
            time_recorded = datetime.now()
            time_recorded = time_recorded.replace(microsecond=0)
            result = [self.id, time_recorded, 'timeout']
            if aggregator != None:
                return self.parser.rpc_call(aggregator, 'add_result', result)
            else:
                self.cache.append(result)
        if subp.poll() == 0:
            time_recorded = datetime.now()
            time_recorded = time_recorded.replace(microsecond=0)
            result = [self.id, time_recorded, 'pass']
            if aggregator != None:
                return self.parser.rpc_call(aggregator, 'add_result', result)
            else:
                self.cache.append(result)
        else:
            time_recorded = datetime.now()
            time_recorded = time_recorded.replace(microsecond=0)
            result = [self.id, time_recorded, 'fail']
            if aggregator != None:
                return self.parser.rpc_call(aggregator, 'add_result', result)
            else:
                self.cache.append(result)
    
    def str2oid(self, soid):
            """
                str2num(soid) -> noid
    
                Convert Object ID "soid" presented in a dotted form into an
                Object ID "noid" represented as a list of numeric sub-ID's.
            """
            # Convert string into a list and filter out empty members
            # (leading dot causes this)
            toid = filter(lambda x: len(x), str.split(soid, '.'))
            noid = map(lambda x: long(x), toid)
            return tuple(noid)
    
    def snmp_poll(self, oid, comm_str, address):
        """
        Called when SNMP protocol set.
        Only polls v2c, converts oid from string.
        """
        rpc_calls = []
        try:
            error_indication, error_status, error_index, var_binds = cmdgen.CommandGenerator().getCmd(
        # SNMP v1
    #    cmdgen.CommunityData('test-agent', 'public', 0),
        # SNMP v2
            cmdgen.CommunityData('orochi', comm_str),
        # SNMP v3
    #    cmdgen.UsmUserData('test-user', 'authkey1', 'privkey1'),
#        cmdgen.UdpTransportTarget((address, 161)), fromString(1,3,6,1,2,1,1,3,0))
            cmdgen.UdpTransportTarget((address, 161)), (self.str2oid(str(oid))))
            
            if error_indication:
                print error_indication
            else:
                if error_status:
                    print '%s at %s\n' % (error_status.prettyPrint(), var_binds[int(error_index)-1])
                else:
                    time_recorded = datetime.now()
                    time_recorded = time_recorded.replace(microsecond = 0)
                    for name, val in var_binds:
                        try:
                            val = val.__int__()
                        except AttributeError:
                            try:
                                val = val.__float__()
                            except AttributeError:
                                val = val.__str__()
                        aggregator = self.poller.get_aggregator()
                        result = [self.id, time_recorded, val]
                        if aggregator != None:
                            rpc_calls.append(self.parser.rpc_call(aggregator, 'add_result', result))
                        else:
                            self.cache.append(result)
        except NoSuchObjectError:
            self.log.error('Can\'t resolve the provided OID')
        if len(rpc_calls) > 0:
            return rpc_calls


    """ Called to execute a libvirt poll """
    def libvirt_poll(self, driver, domain, attr):
        try:
            conn = libvirt.openReadOnly(driver)
        
        except libvirt.libvirtError:
            self.log.error('Failed to connect to the hypervisor')
            
        else:
            try:
                dom = conn.lookupByName(domain)
                attr = getattr(dom, attr)
                val = attr()
                time_recorded = datetime.now()
                time_recorded = time_recorded.replace(microsecond = 0)
                
                result = [self.id, time_recorded, val]
                aggregator = self.poller.get_aggregator()
                if aggregator != None:
                    return self.parser.rpc_call(self.poller.get_aggregator(), 'add_result', result)
                else:
                    self.cache.append(result)
            except libvirt.libvirtError:
                self.log.error('Failed to find domain')

class Poller:
    """ Poller object, establish connection, MUCs and handlers """
    def __init__(self, segment='skynet'):
        config = Configuration()

        self.jobs = {}

        self.sched = MessageScheduler(self.message_handler)
        self.parser = Parser()
        
        self.aggregator = None
        self.failed_aggregator = False
        
        self.query_queue = []
        conn = Connection('poller', 'roflcake')        
        self.entity_prefix, entity_suffix = conn.get_entity_name()
        
#        self.entity_name = entity_prefix + entity_suffix
        conn.join_muc('pollers')
        
        self.segment = segment
    
        self.conn = conn.get_conn()
        
        self.log = Logging(conn)
        self.roster = self.conn.getRoster()
        
        self.conn.RegisterHandler('presence',self.presence_handler)
        
        self.conn.RegisterHandler('iq', self.result_handler, 'result')
        self.conn.RegisterHandler('iq', self.error_handler, 'error')
        self.conn.RegisterHandler('iq', self.get_handler, 'get')
        self.conn.RegisterHandler('iq', self.set_handler, 'set')
        
        self.go_on()
        
    """ Message scheduling handler """
    def message_handler(self, message, retry=False):
        if retry == True:
            self.log.error('Timed out, attempting to resend.')
        self.conn.send(message)
#
#  Handlers for node communication
# 
    """ Presence stanza handler """
    def presence_handler(self, conn, presence_node):
        sender = presence_node.getFrom()
        if presence_node.getAttr('type') == 'subscribe':
            self.roster.Authorize(sender)
#        if sender != self.entity_name:
#            print presence_node.getRole()
#            conn.send(Iq('get', NS_DISCO_INFO, to=presence_node.getFrom()))            
        raise NodeProcessed
    
    """ IQ result handler, acknowledges messages with scheduler """
    def result_handler(self, conn, iq_node):
        iq_id = iq_node.getAttr('id')
        sender = iq_node.getFrom()
        
        if sender.getNode() != self.entity_prefix:
#            pass
            self.sched.received_response(iq_node)
        raise NodeProcessed  # This stanza is fully processed
    
    """ IQ error handler """
    def error_handler(self, conn, iq_node):
        if iq_node.getFrom() == self.aggregator:
            pass
#            print 'Erk!'
        raise NodeProcessed
    
    """ IQ get handler """
    def get_handler(self, conn, iq_node):
        if iq_node.getQueryNS() == NS_DISCO_INFO:
            reply = iq_node.buildReply('result')
            if self.segment != None:
                category = self.segment
            else:
                category = 'skynet'
            identity = Node('identity', {'category':category, 'type':'poller'})
            reply.setQueryPayload([identity])
            conn.send(reply)
        else:
            conn.send(iq_node.buildReply('error'))
        raise NodeProcessed
        
    """ IQ set handler, used for RPC calls, permits methods in whitelist """
    def set_handler(self, conn, iq_node):
            sender = iq_node.getFrom()
            if sender.getNode() != self.entity_prefix:
                query_node = iq_node.getQueryChildren()
                for node in query_node:
                    try:
                        method = node.getTagData('methodName')
                        method_whitelist = ['run_job', 'set_aggregator', 'remove_job', 'aggregator_failure']
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
#                                print sys.exc_info()
                                conn.send(iq_node.buildReply('error'))
                        else:
                            #print 'Method not in whitelist'
                            #print sys.exc_info()
                            conn.send(iq_node.buildReply('error'))
                    except AttributeError:
                        #print sys.exc_info()
                        conn.send(iq_node.buildReply('error'))         
            raise NodeProcessed
    
    #
    # RPC METHODS (job setup and scheduling)
    #
    """ Called by Aggregator to establish job """
    def run_job(self, sender, aggregator, id, addr, proto, freq, dom, resource):
        try:
            job = Job(self, id, addr, proto, freq, dom, resource, self.conn, self.sched)
            self.jobs[id] = job
            job.start()
            return 'success', [int(id)]
        except:
            return 'failure', ['Failed to schedule job']
            
    """ Controller calls to notify of parent Aggregator failure """
    def aggregator_failure(self, sender):
        self.aggregator = None
        self.failed_aggregator = True
        return 'success', ['Removed parent aggregator']
            
    """ Called when job moved off this Poller """
    def remove_job(self, sender, job_id):
        try:
            job = self.jobs[int(job_id)]
            job.end()
            return 'success', ['Stopped job %s' % job_id]
        except:
            return 'failure', ['Failed to stop job %s' % job_id]
    
    """ Called by parent Aggregator, sets where add_results will be sent """
    def set_aggregator(self, sender, aggregator):
        self.log.info('Setting aggregator %s' % aggregator)
        self.aggregator = aggregator
        if self.failed_aggregator == True:
            self.failed_aggregator = False
            for job in self.jobs.values():
                job.send_cached_results()
        return 'success', ['Successfully set aggregator']
            
    #
    # PRIVATE METHODS
    #
    """ Provides parent Aggregator, used by Job instances """
    def get_aggregator(self):
        return self.aggregator

    """ Setup listener """
    def step_on(self):
        try:
            self.conn.Process(1)
        except KeyboardInterrupt:
            server = 'quae.co.uk'
            features.unregister(self.conn, server)
            # Stop all running jobs
            for job in self.jobs.values():
                job.end()
                
            self.sched.end()
            
            print 'Unregistered from %s' % server
            return 0
        return 1
    
    def go_on(self):
        while self.step_on(): pass
    
        
if __name__=='__main__':
    
    poller = Poller()
