from sqlalchemy import *
from utils import Configuration
from datetime import datetime, timedelta

class Database:
    """ Connection to the model """
    def __init__(self):
        config = Configuration()
        server, database, username, password = config.get_db_details()
        # Connect with the provided credentials, recycle every hour to stop idle timeout
        self.db = create_engine('mysql://' + username + ':' + password + '@' + server + '/' + database, pool_recycle=3600)
        self.metadata = MetaData(self.db)
        
    """ Return the selected table from the database """
    def get_table(self, table):
        return Table(table, self.metadata, autoload=True)

# Group operations
    """ Return a group of specific name """
    def get_group_by_name(self, name):
        groups = self.get_table('groups')
        return groups.select(groups.c.name == name).execute().fetchone()

    """ Return a group of specific id """
    def get_group_by_id(self, id):
        groups = self.get_table('groups')
        return groups.select(groups.c.id == id).execute().fetchone()
    
    """ Return all groups """
    def get_groups(self):
        groups = self.get_table('groups')
        group_list = groups.select().execute().fetchall()
        return group_list

    """ Create a group with the given arguments """
    def create_group(self, name, desc):
        groups = self.get_table('groups')
        groups.insert().execute(name=name, description=desc)

    """ Update a group with given ID """
    def update_group(self, id, name, desc):
        groups = self.get_table('groups')
        groups.update(groups.c.id == id).execute(name=name, description=desc)
        
    """ Remove a group by name """
    def remove_group_by_name(self, name):
        groups = self.get_table('groups')
        groups.delete(groups.c.name == name).execute()

# Monitor operations
        
    """ Return a monitor by name """
    def get_monitor(self, name):
        monitors = self.get_table('monitors')
        monitor = monitors.select(monitors.c.name == name).execute().fetchone()
        if monitor != None:
            return monitor
        return False
    
    """ Return monitors, optionally limit by group name """
    def get_monitors(self, group_name=None):
        monitors = self.get_table('monitors')
        if group_name != None:
            groups = self.get_table('groups')
            group = groups.select(groups.c.name == group_name).execute().fetchone()
            retrieved_monitors = monitors.select(monitors.c.group == group.id).execute().fetchall()
        else:
            retrieved_monitors = monitors.select().execute().fetchall()
        return retrieved_monitors
        
    def get_monitors_by_gid(self, group_id):
        monitors = self.get_table('monitors')
        if group_id != None:
            groups = self.get_table('groups')
            retrieved_monitors = monitors.select(monitors.c.group == group_id).execute().fetchall()
        else:
            retrieved_monitors = monitors.select().execute().fetchall()
        return retrieved_monitors
    
    """ Create a monitor with given name, description and group id """
    def create_monitor(self, name, description, group):
        monitors = self.get_table('monitors')
        if monitors.select(monitors.c.name == name).execute().fetchone() == None:
            if group != None:
                groups = self.get_table('groups')
                group = groups.select(groups.c.name == group).execute().fetchone()
                group = group.id
            monitors.insert().execute(name=name, group=group, description=description)
            return True
        return False
        
# In development
    """ Update an existing monitor of ID """
    def update_monitor(self, id, name, group, description):
        monitors = self.get_table('monitors')
        existing = monitors.select(monitors.c.name == name, monitors.c.id != id).execute().fetchall()
        
    """ Remove monitor by name """
    def remove_monitor(self, name):
        monitors = self.get_table('monitors')
        
# job operations
    """ Return a job by ID """
    def get_job(self, id, monitor=None):
        jobs = self.get_table('jobs')
        if monitor != None:
            print monitor
            monitor = self.get_monitor(monitor)
            query = and_(jobs.c.monitor == monitor.id, jobs.c.id == id)
        else:
            query = jobs.c.id == id
        job = jobs.select(query).execute().fetchone()
        if job != None:
            return dict(job)
        return False
        
    """ Return all jobs for a specified monitor """
    def get_jobs(self, monitor):
        jobs = self.get_table('jobs')
        monitor = self.get_monitor(monitor)
        job_list = jobs.select(jobs.c.monitor == monitor.id).execute().fetchall()
        return job_list
        
    """ Create a job with provided details """
    def create_job(self, address, protocol, frequency, domain, interface, mon):
        monitor = self.get_monitor(mon)
        if monitor != False:
            jobs = self.get_table('jobs')
            jobs.insert().execute(address=address, protocol=protocol, frequency=frequency, domain=domain, interface=interface, monitor=monitor.id)
            return True
        return False
      
    """ Update a job by ID """ 
    def update_job(self, id, address, protocol, frequency, domain, resource):
        jobs = self.get_table('jobs')
        jobs.update(jobs.c.id == id).execute({'address':address, 'protocol':protocol, 'frequency':frequency, 'domain':domain, 'resource':resource})

    """ Remove a job of id """
    def remove_job(self, mon_name, id):
        pass

# Evaluation operations
    """ Return all evaluations for a specific job id """
    def get_evaluations(self, job):
        evaluations = self.get_table('evaluations')
        job_evals = evaluations.select(evaluations.c.job == job).execute().fetchall()
        return job_evals
        
# Result operations
    """ Return results for a specified job between two datetime stamps """
    def get_results(self, monitor, job_id, start_datetime, end_datetime):
        job = self.get_job(job_id, monitor)
        if job != False:
            results_table = self.get_table('results')
            s = select([results_table.c.recorded, results_table.c.int, results_table.c.string, results_table.c.float], and_(results_table.c.job == job_id, results_table.c.recorded > start_datetime, results_table.c.recorded < end_datetime))
            results = s.execute().fetchall()
            if len(results) > 0:
                # Using indexes as counter as ints and string are immutable
                for i in range(0, len(results)):
                    result = results[i]
                    result = dict(result.items())
                    if result.get('int') != None:
                        result.pop('string')
                        result.pop('float')
                    elif result.get('string') != None:
                        result.pop('int')
                        result.pop('float')
                    elif result.get('float') != None:
                        result.pop('int')
                        result.pop('string')
                    results[i] = result
                return [results]
            else:
                return None
        return False
        
    """ Return results for a day for a specified job starting at a provided timestamp """
    def get_results_day(self, monitor, job_id, start_datetime):
        job = self.get_job(job_id, monitor)
        start_datetime = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S").replace(hour=0)
        if job != False:
            results = []
            results_table = self.get_table('results')
            for i in range(0,23):
                loop_datetime = start_datetime + timedelta(hours=i)
                end_datetime = loop_datetime + timedelta(hours=1)
                #s = select([results_table.c.recorded, results_table.c.string], [func.avg(results_table.c.int), func.avg(results_table.c.float)], and_(results_table.c.job == job_id, results_table.c.recorded > start_datetime, results_table.c.recorded < end_datetime))
                s = select([results_table.c.recorded, func.avg(results_table.c.int), func.avg(results_table.c.float), func.avg(results_table.c.string)], and_(results_table.c.job == job_id, results_table.c.recorded > loop_datetime, results_table.c.recorded < end_datetime), group_by=results_table.c.job)
                result = s.execute().fetchone()
                results.append(result)
                print result
            if len(results) > 0:
                agg_results = []
                # Using indexes as counter as ints and string are immutable
                for i in range(0, len(results)):
                    result = results[i]
                    if result != None:
                        recorded, int_val, float_val, str_val = result
                        if int_val != None:
                            result_val = int(int_val)
                        elif float_val != None:
                            result_val = float(float_val)
                        elif str_val != None:
                            result_val = str(str_val)
                        agg_results.append({'recorded':recorded, 'result':result_val})
                return agg_results
            else:
                return None
        return False
        
    """ Return results for an hour for a specified job starting at a provided timestamp """
    def get_results_hour(self, monitor, job_id, start_datetime):
        job = self.get_job(job_id, monitor)
        start_datetime = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S")
        if job != False:
            results = []
            results_table = self.get_table('results')
            for i in range(0,55,5):
                start_datetime = start_datetime.replace(minute=i)
                end_datetime = start_datetime.replace(minute=start_datetime.minute+5)
                #s = select([results_table.c.recorded, results_table.c.string], [func.avg(results_table.c.int), func.avg(results_table.c.float)], and_(results_table.c.job == job_id, results_table.c.recorded > start_datetime, results_table.c.recorded < end_datetime))
                s = select([results_table.c.recorded, func.avg(results_table.c.int), func.avg(results_table.c.float), func.avg(results_table.c.string)], and_(results_table.c.job == job_id, results_table.c.recorded > start_datetime, results_table.c.recorded < end_datetime), group_by=results_table.c.job)
                result = s.execute().fetchone()
                results.append(result)
                print result
            if len(results) > 0:
                agg_results = []
                # Using indexes as counter as ints and string are immutable
                for i in range(0, len(results)):
                    result = results[i]
                    if result != None:
                        recorded, int_val, float_val, str_val = result
                        if int_val != None:
                            result_val = int(int_val)
                        elif float_val != None:
                            result_val = float(float_val)
                        elif str_val != None:
                            result_val = str(str_val)
                        agg_results.append({'recorded':recorded, 'result':result_val})
                return agg_results
            else:
                return None
        return False
        
# Segement operations
    """ Return all network segments """
    def get_segments(self):
        segments = self.get_table('segments')
        return segments.select().execute().fetchall()

    """ Return the name of a segment with ID seg_id """
    def get_segment_name(self, seg_id):
        segments = self.get_table('segments')
        segment = segments.select(segments.c.id == seg_id).execute().fetchone()
        if segment != None:
            segment = segment.name
        return segment
