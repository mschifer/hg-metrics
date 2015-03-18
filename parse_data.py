import json
import os
import sys
import argparse
from array import *
from churnhash2 import ChurnHash
from backend import SQLiteBackend
import pprint

branch_list = ""

def parse_data():
    global _backend
    list = []
    file_exts = ['.cpp','.h','.xml','.js','.css','.java','.jsm','.json','.xhtml','.html','.c','.asm','.idl','.xul',]
    
    
    
    f = open(args.branch_list, 'r')
    
    
    
    for line in f:
        list.append(line.rstrip())
    
    output = {}
    for release in list:
    
        fp = open('%s.json' % release, "r")
        print 'opened file %s.json'  % release
    
        history = json.load(fp)
        fp.close()
        ch = ChurnHash()
    
        for i in history:
            for j in history[i]["files"]:
                if "filename" in j:
                    # This needs to be an command line / config option
                    fname = "/Users/mschifer/mozilla-central/%s" % j["filename"]
                    if os.path.isfile(fname):
                       num_lines = sum(1 for line in open(fname))
                    else:
                       num_lines = 1 
                    #pprint.pprint(history[i])
                    if ("bug" in history[i].keys()):
                        bugnumber = history[i]["bug"]
                    else:
                        bugnumber = 0;
                    ch.add_file_path(i, j["filename"], j["added"], j["removed"], num_lines, bugnumber)
    
    
        h = ch.get_hash()
        for i in h:
          fileName, fileExtension = os.path.splitext(h[i]['file'])
          if  ( fileExtension in file_exts):
              if (float(h[i]['lines_total']) > 0):
                  percent_change =  ( ( float( h[i]['lines_added']) + float( h[i]['lines_removed'] )) / float(h[i]['lines_total'] ) ) * 100
              else:
                  percent_change = -1

              if percent_change > 100:
                  percent_change = 100
              if i not in output:
                 output[i] = {}
              if release not in output[i]:
                 output[i][release] = {}
              output[i][release]['commit_id'] = h[i]['commit_id']
              output[i][release]['delta'] = h[i]['lines_added'] + h[i]['lines_removed']
              output[i][release]['percent'] = percent_change
              output[i][release]['lines_total'] = h[i]['lines_total']
              output[i]['file'] = '%s' % h[i]['file']
              output[i][release]['bug'] = '%s' % h[i]['bug']
    
    n_header = 'File'
    a_header = 'File'
    b_header = 'File'
    release_ids = {}
    file_ids    = {}
    
    for release in list:
        # for each release check the file stats and update the database
        rel_id =  _backend.get_release_id(release)
        if len(rel_id) == 0:
           print 'Release Not Found: Adding Release'
           rel_id =   _backend.add_release_values(release)
        release_ids[release] = int(rel_id[0][0])
       
        for i in output:
            # check if file is in database, if not add it and get ID.
            file_id = _backend.get_file_id(output[i]['file'])
            if len(file_id) == 0:
               file_id = _backend.add_file_values(output[i]['file'])
            file_ids[output[i]['file']] = file_id[0][0]
            
            # add the file data for this release
            if release in output[i]: 
                # Check if commit has been added yet or not
                commits = _backend.get_commit_id(output[i][release]['commit_id'])
                print 'COMMITS'
                pprint.pprint(commits)
                if len(commits) == 0:
                    _backend.add_change_values(file_id[0][0], release_ids[release], 
                        output[i][release]['delta'] , output[i][release]['lines_total'], 
                        output[i][release]['percent'], output[i][release]['bug'], output[i][release]['commit_id'])
                else:
                    print 'Commit ID: ', output[i][release]['commit_id'], ' Already Processed - SKIPPING'


            else:
                _backend.add_change_values(file_id[0][0],release_ids[release] , 0 , 0, 0, 0, 0 )
        

def process_data():
    global _backend
    # Calculate average change per release
    # foreach file
    all_files = _backend.get_file_ids()
    all_releases = _backend.get_release_ids()
    for file_id in  all_files:
        #print "FILE ID"
        #pprint.pprint(file_id[0])
        change_rate  = _backend.get_changes_by_file(file_id[0])
        change_data = dict();
        file_data = dict()
        change_list = []
        release_id = 0;
        for row in  change_rate:
                release_id = row[2]
                file_data[release_id] = row[3]
                #pprint.pprint(file_data)
                change_list.append(row[3])
        change_data[file_id[0]] = file_data;
        print "==============================================="
        pprint.pprint(change_data)
        print "==="
        pprint.pprint(change_list)
        print "==============================================="
        mean,stdev = meanstdv(change_list) 
        print 'FILE ID:', file_id[0]
        print 'MEAN   :', mean
        print 'STDEV  :', stdev
        
        _backend.update_avg_change(file_id[0], mean, stdev)
        if len(change_list) == 0:
            print 'Change List is NULL'
        else:
            print 'FILE ID:', file_id[0], ' STDV:', meanstdv(change_list), ' MAX CHANGE:', max(change_list) 
            if ( max(change_list)  > (mean + stdev) ):
                for  rel_id, change  in change_data[file_id[0]].items() :
                    if change == max(change_list):
                       print 'HIGH CHANGE RATE for file ', file_id[0], ' in release ', rel_id

"""
Calculate mean and standard deviation of data x[]:
    mean = {\sum_i x_i \over n}
    std = sqrt(\sum_i (x_i - mean)^2 \over n-1)
"""
def meanstdv(x):
    from math import sqrt
    n, mean, std = len(x), 0, 0
    if n == 0:
        return 0,0
    for a in x:
	mean = mean + a
    mean = mean / float(n)
    for a in x:
	std = std + (a - mean)**2
    if n == 1:
       return mean,0
    std = sqrt(std / float(n-1))
    return mean, std

############################################

# Main 
if __name__ == '__main__':

    # Parse Command Line Arguments
    """ HG Metrics Parse Data:
    Command Line options
    parse_data.py [-b filename]
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", action='store', dest="branch_list",  help="File containing branch list")
    args = parser.parse_args()
    _backend = SQLiteBackend()
    parse_data()
    process_data()
