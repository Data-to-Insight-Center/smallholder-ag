#!/usr/bin/env python

import collections
import os
import sys
import json
import six
import csv, codecs, cStringIO
from collections import OrderedDict

class UnicodeWriter:
    """
    Copied from Python API Doc (https://docs.python.org/2.7/library/csv.html)
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        self.writer.writerow([s.encode("utf-8") for s in row])
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

def _json_to_csv_(json_object, csv_file):    
    """
    Adapted from csvkit
    (https://github.com/onyxfish/csvkit/blob/61b9c208b7665c20e9a8e95ba6eee811d04705f0/csvkit/convert/js.py)
    
    This is the same algorithm behind http://konklone.io/json/
    (https://github.com/konklone/json/blob/gh-pages/assets/site.js)
    """    
    key = [key for key in json_object.keys() if isinstance(json_object[key], list)]
    if len(key) != 1:
        raise TypeError("The JSON document is expected to have one and only one list item.")
    
    fields = []
    rows = []
    
    items = json_object[key[0]]
    for item in items:        
        row = _parse_object_(item)                
        rows.append(row)
        for key in row.keys():
            if key not in fields:
                fields.append(key)
    
    # Note: the python csv module does not support unicode string        
    file = open(csv_file, 'w+')    
    csv_writer = UnicodeWriter(file)
    csv_writer.writerow(fields)
    for row in rows:
        final_row = []
        for field in fields:
            final_row.append(row.get(field, ""))
        csv_writer.writerow(final_row)

    file.close()
    
        
def _parse_object_(obj, path=''):
    """
    Adapted from csvkit 
    (https://github.com/onyxfish/csvkit/blob/61b9c208b7665c20e9a8e95ba6eee811d04705f0/csvkit/convert/js.py#L15)
    """
    if isinstance(obj, dict):
        iterator = obj.items()
    elif isinstance(obj, (list, tuple)):
        iterator = enumerate(obj)
    else:           
        return { path.strip('/'): unicode(obj) }

    d = OrderedDict()

    for key, value in iterator:
        key = six.text_type(key)
        d.update(_parse_object_(value, path + key + '/'))

    return d
        

def main(args):
    if (len(args) != 2):
        print ("Usage: json2csv.py <json files directory> <csv files directory>")
        sys.exit() 
    
    input_dir = args[0]
    output_dir = args[1]
    
    for filename in os.listdir(input_dir):
        filepath = os.path.join(input_dir, filename)
        if ((not (os.path.isdir(filepath))) and filename.endswith("json")):
            # load json file
            file = open(filepath, 'r')
            json_string = file.read()
            file.close()
            
            # transform the json string to a json object
            json_object = json.loads(json_string, object_pairs_hook=collections.OrderedDict)
            
            # convert the json object to a csv file
            print "Converting ", filename
            _json_to_csv_(json_object, os.path.join(output_dir, filename.replace('json', 'csv')))

if __name__ == '__main__':  
    main(sys.argv[1:])