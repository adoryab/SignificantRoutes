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
                 confirm=None, tries=0, skips=0, connects=0,
                 interpret=None, translator=dict(), processor=None,
                 append="", key_id='key'):
        """Constructor for Service class

        PARAMETERS
        name: name of service
        URL: URL for service

        OPTIONAL
        required: required parameters for API calls
        (do not include 'key' in required; handle that through keys)
        optional: optional parameters for API calls
        keys: keys for API calls
        confirm: confirmation method for successful API calls
        (needed for key rotation; either list or dictionary)
        tries: attempts to make before declaring all keys dead
        skips: calls to skip in case call, not key, is failing
        connects: connection attempts to be made before giving up
        interpret: a function to sort through JSON output
        translator: dictionary to convert names
        processor: a function to generate input parameters from database output
        (must also have a variable "error" to enable/disable errors!)
        append: a string to append to all resulting fields
        key_id: ID of key parameter, if not 'key' (default)

        If you're providing keys but would like a keyless option
        to exist as well, simply include None in keys"""

        self.name, self.URL, self.key_id = name, URL, key_id
        self.translator, self.interpret = translator, interpret
        self.processor, self.append = processor, append
        self.attempts, self.tries = 0, tries
        self.skipped, self.skips = 0, skips
        self.connectAttempts, self.connects = 0, connects
        self.confirmed, self.confirmation = True, None
        if (confirm is not None):
            self.confirmation, self.confirmed = confirm, False
        self.keys, self.usesKeys = keys, False
        if len(self.keys != 0): 
            self.keyIndex, self.usesKeys = 0, True
        self.params = dict()
        params['required'], params['optional'] = required, optional

    def __repr__(self):
        return (self.name, self.URL)

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
            success, self.connects = False, 0
            while (not success and self.connectAttempts <= self.connects):
                try:
                    req = requests.get(url=URL, params=callParams)
                    success = True
                except:
                    self.connectAttempts += 1
            self.confirm(req)
            if (not self.confirmed): self.rotate()
        try:
            return req.json()
        except:
            return dict()

    def translate(self, call):
        for item in self.translator:
            if item in call:
                value = call[item]
                newName = self.translator[item] + self.append
                call[newName] = value
                del call[item]
        return call

    def process(self, call):
        if self.interpret is not None:
            try:
                return self.translate(self.interpret(call))
            except:
                raise IOError("Interpret function failed!")
        else:
            return self.translate(call)

    def getData(self, inputs, processorError=True, paramError=False):
        """Makes an API call and gets JSON results

        PARAMETERS
        inputs: a dictionary of inputs

        OPTIONAL
        processorError: if set to True, raises error
        if processor function fails for reasons unrelated
        to parameter availability
        paramError: if set to True, raises error 
        if not all parameters are available"""

        callParams = dict()
        if self.processor is not None:
            try:
                inputs = self.processor(inputs, error=paramError)
            except:
                if processorError:
                    raise IOError("Processor function failure- input may not have all required parameters!")
        for item in params['required']:
            if item in inputs:
                callParams[item] = inputs[item]
            else:
                if paramError:
                    raise IOError("Input does not have all required parameters!")
                return dict()
        for item in params['optional']:
            if item in inputs:
                callParams[item] = inputs[item]
        if self.key_id in callParams: del callParams[key_id] # avoids duplicates or confusion
        if self.usesKeys: callParams[key_id] = self.keys[self.keyIndex]
        if callParams[key_id] is None: del callParams[key_id]
        return self.process(self.makeCall(URL, callParams))