# service.py
# module to handle API services

import requests

# EXCEPTIONS

class StatusError(Exception): 
    """Raised when API calls fail"""
    pass

# CLASSES

class Service(object):
    """Object to call API services"""

    def __init__(self, name, URL, required=[], optional=[], keys=[], 
    	         confirm=None, tries=0, skips=0, interpreter=[],
    	         translator=dict()):
        """Constructor for Service class
        PARAMETERS
        name: name of service
        URL: URL for service

        OPTIONAL
        required: required parameters for API calls
        optional: optional parameters for API calls
        keys: keys for API calls
        confirm: confirmation method for successful API calls
        (needed for key rotation; either list or dictionary)
        tries: attempts to make before declaring all keys dead
        skips: calls to skip in case call, not key, is failing
        interpreter: a list of strings and ints to sort through JSON output
        translator: dictionary to convert names

        If you're providing keys but would like a keyless option
        to exist as well, simply include None in keys"""

        self.name, self.URL = name, URL
        self.translator, self.interpreter = translator, interpreter
        self.attempts, self.tries = 0, tries
        self.skipped, self.skips = 0, skips
        self.confirmed, self.confirmation = True, None
        if (confirm is not None):
            self.confirmation, self.confirmed = confirm, False
        self.keys, self.usesKeys = keys, False
        if len(self.keys != 0): 
            self.keyIndex, self.usesKeys = 0, True
        self.params = dict()
        params['required'], params['optional'] = required, optional

    def __repr__(self):
        return self.URL

    def confirm(self, request):
    	self.confirm = True
        if type(self.confirmation) == list:
            for item in self.confirmation:
                if item not in request:
                	self.confirmed = False
        elif type(self.confirmation) == dict:
        	for item in self.confirmation:
        		if item not in request:
        			self.confirmed = False
        		else:
        			if type(self.confirm[item] == list):
        				if request[item] not in self.confirmation[item]:
        					self.confirmed = False
        			else:
        				if self.confirmation[item] != request[item]:
        					self.confirmed = False

    def advance(self):
		self.skipped += 1
	    if (self.skipped > self.skips):
	    	raise StatusError("All calls and keys failed!")
	    else:
	    	self.confirmed = True

    def rotate(self):
    	if self.usesKeys:
    		self.attempts += 1
    		if (self.attempts > self.tries):
	    		self.advance()
	    		self.attempts = 0
	    	else:
	    		self.keyIndex += 1
	    		self.keyIndex = self.keyIndex%(len(self.keys))
	    else:
	    	self.advance()

    def makeCall(URL, parameters):
    	self.confirmed = False
        while (not self.confirmed):
            req = requests.get(url=URL, params=callParams)
            self.confirm(req)
            if (not self.confirmed): self.rotate()
        return req.json()

    def interpret(self, call):
    	for item in self.interpreter:
    		call = call[item]
    	return call

    def translate(self, call):
    	for item in self.translator:
    		value = call[item]
    		call[self.translator[item]] = value
    		del call[item]
    	return call

    def process(self, call):
    	return self.translate(self.interpret(call))

    def getData(self, inputs):
        """Makes an API call and gets JSON results

        PARAMETERS
        inputs: a dictionary of inputs"""

        callParams = dict()
        for item in params['required']:
            if item in inputs:
                callParams[item] = inputs[item]
            else:
                raise IOError("Input does not have all required parameters!")
        for item in params['optional']:
            if item in inputs:
                callParams[item] = inputs[item]
        if 'key' in callParams: del callParams['key']
        if self.usesKeys: callParams['key'] = self.keys[self.keyIndex]
        if callParams['key'] is None: del callParams['key']
        return self.process(self.makeCall(URL, callParams))