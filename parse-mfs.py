import json
import os
import sys
import argparse
from array import *
from churnhash2 import ChurnHash



parser = argparse.ArgumentParser()
parser.add_argument("-b", action='store', dest="branch_list",  help="File containing branch list")
parser.add_argument("-c", help="moo")

args = parser.parse_args()

list = []

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
        print 'I:%s' % i
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
        print 'ZZZ:Release:%s File:%s A:%s R:%s D:%s H:%s' % (release,  h[i]['file'], h[i]['lines_added'], h[i]['lines_removed'], int( h[i]['lines_added']) + int( h[i]['lines_removed']), i )
        if i not in output:
           output[i] = {}
        if release not in output[i]:
           output[i][release] = {}
        output[i][release]['delta'] = h[i]['lines_added'] + h[i]['lines_removed']
        output[i][release]['percent'] = percent_change
        output[i][release]['lines_total'] = h[i]['lines_total']
        output[i]['file'] = '%s' % h[i]['file']

# Output in csv format

nfp = open('nightly-0.csv', "w")
header = 'File'
for release in list:
        header += ',%s' % release
header += '\n'
nfp.write(header)

print 'FOR EACH OUTPUT!'
for i in output:
    print 'XXX:%s' % output[i]
    print 'XX-:%s' % output[i]['file']
    nfp.write(output[i]['file'])
    for release in list:
        if release in output[i]: 
           nfp.write(", %s %s %s %s" % (release,output[i][release]['delta'],output[i][release]['lines_total'],output[i][release]['percent']))
        else:
           nfp.write(", %s %s %s %s" % (release,0,0,0))
    nfp.write('\n') 
nfp.close()
        

#for i in h:
#    if (h[i]['file'].count('/') <=3 and (h[i]['file'].count('.') == 0)):
#        ofp.write("%s,%d,%d\n" % (h[i]['file'], h[i]['lines_added'], h[i]['lines_removed']))

#    ofp.close()
