import json
import os
import sys
import argparse
from array import *
from churnhash2 import ChurnHash
from backend import SQLiteBackend



parser = argparse.ArgumentParser()
parser.add_argument("-b", action='store', dest="branch_list",  help="File containing branch list")
parser.add_argument("-c", help="moo")

args = parser.parse_args()

list = []

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
        #print 'I:%s' % i
        for j in history[i]["files"]:
            if "filename" in j:
                fname = "/Users/mschifer/mozilla-central/%s" % j["filename"]
                if os.path.isfile(fname):
                   num_lines = sum(1 for line in open(fname))
                else:
                   num_lines = 1 
                ch.add_file_path(j["filename"], j["added"], j["removed"], num_lines)


    h = ch.get_hash()
    for i in h:
        percent_change =  ( ( float( h[i]['lines_added']) + float( h[i]['lines_removed'] )) / float(h[i]['lines_total'] ) ) * 100
        #print 'ZZZ:Release:%s File:%s A:%s R:%s D:%s H:%s' % (release,  h[i]['file'], h[i]['lines_added'], h[i]['lines_removed'], int( h[i]['lines_added']) + int( h[i]['lines_removed']), i )
        if i not in output:
           output[i] = {}
        if release not in output[i]:
           output[i][release] = {}
        output[i][release]['delta'] = h[i]['lines_added'] + h[i]['lines_removed']
        output[i][release]['percent'] = percent_change
        output[i][release]['lines_total'] = h[i]['lines_total']
        output[i]['file'] = '%s' % h[i]['file']

# Output in csv format
#nfp = open('nightly-0.csv', "w")
#afp = open('aurora-0.csv', "w")
#bfp = open('beta-0.csv', "w")

n_header = 'File'
a_header = 'File'
b_header = 'File'
release_ids = {}
file_ids    = {}

for release in list:
    if 'nightly' in release:
       n_header += ', %s' % release
    if 'aurora' in release:
       a_header += ', %s' % release
    if 'beta' in release:
       b_header += ', %s' % release
    rel_id =  _backend.get_release_id(release)
    if len(rel_id) == 0:
       print 'Release Not Found: Adding Release'
       rel_id =   _backend.add_release_values(release)
    print "REL_ID1: %d" % rel_id[0]
    release_ids[release] = int(rel_id[0][0])
    print 'REL_ID2: %d' % release_ids[release]

print release_ids
for k in release_ids:
    print 'K: %s %d'  % ( k,int(release_ids[k]))
#n_header += '\n'
#a_header += '\n'
#b_header += '\n'
#
#nfp.write(n_header)
#afp.write(a_header)
#bfp.write(b_header)
#
for i in output:
     # check if file is in database, if not add it and get ID.
     file_id = _backend.get_file_id(output[i]['file'])
     if len(file_id) == 0:
        print 'Adding File to Database'
        file_id = _backend.add_file_values(output[i]['file'])
     print 'File ID: %s %s' % (file_id,output[i]['file'])
     file_ids[output[i]['file']] = file_id[0][0]
     
     # add thef file data for this release
     _backend.add_change_values(file_id[0][0],release_ids[release],output[i][release]['delta'] , output[i][release]['lines_total'] , output[i][release]['percent'])
    
#    nfp.write(output[i]['file'])
#    afp.write(output[i]['file'])
#    bfp.write(output[i]['file'])
#    for release in list:
#        if release in output[i]: 
#           csv_entry = ", %s %s %s %s" % (release,output[i][release]['delta'],output[i][release]['lines_total'],output[i][release]['percent'])
#        else:
#           csv_entry = ", %s %s %s %s" % (release,0,0,0)
#
#        if 'nightly' in release:
#            nfp.write(csv_entry)
#        if 'aurora' in release:
#            afp.write(csv_entry)
#        if 'beta' in release:
#            bfp.write(csv_entry)
#    nfp.write('\n')
#    afp.write('\n')
#    bfp.write('\n')
#
#nfp.close()
#afp.close()
#bfp.close()
#
#
#
#
##for i in h:
##    if (h[i]['file'].count('/') <=3 and (h[i]['file'].count('.') == 0)):
##        ofp.write("%s,%d,%d\n" % (h[i]['file'], h[i]['lines_added'], h[i]['lines_removed']))
#
