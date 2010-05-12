from xmpp import *
import sys, random, time, base64, urllib2, smtplib
from email.mime.text import MIMEText
from ConfigParser import ConfigParser
from threading import Timer, Semaphore
from datetime import datetime

class Parser:
    """ Parser object """
    def __init__(self):
        self.temp_list = []
        
    def type_to_rpc(self, py_type):
        rpc_type = None
                
        if py_type == 'str' or py_type == 'unicode':
            rpc_type = 'string'
        if py_type == 'long' or py_type == 'int':
            rpc_type = 'int'
        if py_type == 'datetime':
            rpc_type = 'dateTime.iso8601'
            
        return rpc_type
        
    """ Converts XML-RPC values to python type """
    def rpc_to_type(self, rpc_type, value):
        py_val = None
        
        if rpc_type == 'string':
            py_val = str(value)
        elif rpc_type == 'int':
            py_val = int(value)
        elif rpc_type == 'dateTime.iso8601':
            py_val = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return py_val

    """ Returns Python data-type/struct from XML """
    def get_values(self, parent, param):
        if param.getName() == 'member' and param.getTag('name') != None:
            name = str(param.getTag('name').getData())
            param.delChild('name')
            self.get_values((parent, name), param)

            
        else:
            values = param.getChildren()
            
            for value in values:
                value_types = value.getChildren()
                for value_type in value_types:
                    
                    if value_type.getName() == 'array':
                        array_list = []
                        parent_type = type(parent).__name__
                        if parent_type == 'list':
                            parent.append(array_list)
                        else:
                            parent.update(array_list)
                            
                        for data_node in value_type.getChildren():
                            self.get_values(array_list, data_node)
                    elif value_type.getName() == 'struct':
                        struct_dict = {}
                        parent_type = type(parent).__name__
                        if parent_type == 'list':
                            parent.append(struct_dict)
                        else:
                            parent.update(struct_dict)
                        for member in value_type.getChildren():
                            self.get_values(struct_dict, member)
#                    elif value_type.getName() == 'nil':
#                        print value_type.getData()
#                        parent_type = type(parent).__name__
#                        if parent_type == 'tuple':
#                            parent, name =
                    else:
                        converted = self.rpc_to_type(value_type.getName(), value_type.getData())
                        parent_type = type(parent).__name__
#                        if parent_type == 'NoneType':
                        if parent_type == 'tuple':
                            parent, name = parent
                            parent[name] = converted
                        elif parent_type == 'list':
                            parent.append(converted)
            
    """ Encodes from Python type to XML-RPC """
    def create_args(self, parent_node, param):

        if type(param).__name__ == 'RowProxy':
            param = dict(param.items())

        param_type = type(param).__name__
    
        if param_type == 'list':
            val_node = parent_node.addChild('value')
            type_node = val_node.addChild('array')
            data_node = type_node.addChild('data')
            for element in param:
                self.create_args(data_node, element)
        elif param_type == 'dict':
            val_node = parent_node.addChild('value')
            type_node = val_node.addChild('struct')
            for element in param.items():
                member_node = type_node.addChild('member')
                self.create_args(member_node, element)
            
        else:
            if param_type == 'tuple':
                key, param = param
                param_type = type(param).__name__
                name_node = parent_node.addChild('name')
                name_node.setData(key)
            
            val_node = parent_node.addChild('value')
            
            if param != None:
                rpc_type = self.type_to_rpc(param_type)
                type_node = val_node.addChild(rpc_type)
                # Convert timestamp to ISO 8601 format
                if rpc_type == 'dateTime.iso8601':
                    param = param.isoformat()
                type_node.setData(param)
            
            else:
                type_node = val_node.addChild('nil')
                
    """ Starts recursive call for XML-RPC --> Python data """
    def get_args(self, params, sender=None):
        for param  in params:
            self.get_values(self.temp_list, param)
        args = self.temp_list
        args.insert(0, sender)
        self.temp_list = []
        return args
    
    """ Same as above, doesn't take/return sender """
    def get_args_no_sender(self, params):
        for param  in params:
            self.get_values(self.temp_list, param)
        args = self.temp_list
        self.temp_list = []
        return args
 
    """ Generates XML-RPC fault message """
    def fault_message(self, fault_code, fault_string):
        query = Node('query', attrs={'xmlns':NS_RPC})
        method_response = query.addChild('methodResponse')
    
        fault = method_response.addChild('fault')
        value = fault.addChild('value')
        struct = value.addChild('struct')
        # First member, for fault code
        code_member = struct.addChild('member')
        code_name = code_member.addChild('name')
        code_name.setData('faultCode')
        code_val = code_member.addChild('value')
        code_type = code_val.addChild('int')
        code_type.setData(fault_code)
    
        string_member = struct.addChild('member')
        string_name = string_member.addChild('name')
        string_name.setData('faultString')
        string_val = string_member.addChild('value')
        string_type = string_val.addChild('string')
        string_type.setData(fault_string)
        return query
    
    """ Calls recursive conversion from Python --> XML-RPC """
    def rpc_call(self, to, name, params=[]):
        query = Node('query', attrs={'xmlns':NS_RPC})
        method_call = query.addChild('methodCall')
        method_name = method_call.addChild('methodName')
        method_name.setData(name)
        parameters = method_call.addChild('params')
        for param in params:
            param_node = parameters.addChild('param')
            self.create_args(param_node, param)

        return Protocol('iq', to, 'set', payload=[query])

    """ Creates RPC response """
    def rpc_response(self, to, id, status, params):

        if status == 'success':
            query = Node('query', attrs={'xmlns':NS_RPC})
            method_response = query.addChild('methodResponse')
            parameters = method_response.addChild('params')
# Recurse!
            for param in params:
                param_node = parameters.addChild('param')
                self.create_args(param_node, param) 
#            value = parameter.addChild('value')
#            data = value.addChild(xml_type)
#            data.setData(param)
        else:
            query = self.fault_message('1', params[0])
            
        return Protocol('iq', to, 'result', attrs={'id':id}, payload=[query])

if __name__=='__main__':
    print 'Whoops! This isn\'t meant to be run directly.'
