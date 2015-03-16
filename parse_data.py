import json
import os
import sys
import argparse
from array import *
from churnhash2 import ChurnHash
from backend import SQLiteBackend
import pprint

branch_list = ""

def _parse_data():

    list = []
    file_exts = ['.cpp','.h','.xml','.js','.css','.java','.jsm','.json','.xhtml','.html','.c','.asm','.idl','.xul',]
    
    
    
    f = open(args.branch_list, 'r')
    
    _backend = SQLiteBackend()
    
    
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
                    ch.add_file_path(j["filename"], j["added"], j["removed"], num_lines, bugnumber)
    
    
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
                _backend.add_change_values(file_id[0][0], release_ids[release], 
                    output[i][release]['delta'] , output[i][release]['lines_total'], 
                    output[i][release]['percent'], output[i][release]['bug'])
            else:
                _backend.add_change_values(file_id[0][0],release_ids[release] , 0 , 0, 0, 0 )
        
    # Calculate average change per release
    # foreach file
    #all_files = _backend.get_file_ids()
    #all_releases = _backend.get_release_ids()
    #foreach ( all_files as file_id ):
    #    foreach ( all_releases as release_id):
    #        # query each record from release_changes that match file id and release id
    #        change_rate  = get_changes_by_file(file_id)
    #        foreach ( change_rate as row ):
    #            pprint.pprint(row)
    #
    ##for i in h:
    ##    if (h[i]['file'].count('/') <=3 and (h[i]['file'].count('.') == 0)):
    ##        ofp.write("%s,%d,%d\n" % (h[i]['file'], h[i]['lines_added'], h[i]['lines_removed']))
    #

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
    _parse_data()
