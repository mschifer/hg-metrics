import json
import os
import sys
import argparse
from array import *
from churnhash2 import ChurnHash
from backend import SQLiteBackend
import pprint
import re
import httplib
import urllib
import urllib2
import time
import datetime

# Branchlist format is:
# release-name /path/to/branchsource
# Example:
# nightly-36 /Users/mschifer/mozilla-central
# aurora-36 /Users/mschifer/mozilla-aurora
# beta-36 /Users/mschifer/mozilla-beta
# release-36 /Users/mschifer/mozilla-release


branch_list = ""

def parse_data():
    global _backend
    list = []
    nightly = {}
    aurora  = {}
    beta    = {}
    release = {}
    file_exts = ['.cpp','.h','.xml','.js','.css','.java','.jsm','.json','.xhtml','.html','.c','.asm','.idl','.xul',]
    file_ignores = ['gaia.json','sources.xml']
    #file_ignores = ['test','gaia.json','sources.xml']
    
    
    
    f = open(args.branch_list, 'r')
    
    for line in f:
        branch, path  = line.rstrip().split(' ',1)
        if branch.find('nightly') > -1:
           nightly[branch] = path.lstrip()
        if branch.find('aurora') > -1:
           aurora[branch] = path.lstrip()
        if branch.find('beta') > -1:
           beta[branch] =  path.lstrip()
        if branch.find('release') > -1:
           release[branch] =  path.lstrip()

    list.append(nightly)
    list.append(aurora)
    list.append(beta)
    list.append(release)
    
    # Process each of the json files in the brach list
    # Files must be processed in order of Nightly, Aurora, Beta
    for order in list:
        for release, path in order.items():
            fp = open('%s.json' % release, "r")
    
            # Make sure we have the release in our list and assigned a release id
            rels =  _backend.get_release_id(release)
            if len(rels) == 0:
                #print 'Release Not Found: Adding Release'
                branch,number = release.split('-',1)
                rels  =   _backend.add_release_values(release,number)
                rel_id = rels[0][0]
            else:
                rel_id = rels[0][0]
            #print 'RELID ', rel_id
            print 'opened file %s.json release id:%s'  % (release, rel_id)
    
            # Load the json file 
            history = json.load(fp)
            fp.close()
    
            # Process the data for the release 
            for commit_id in history:
                for file_data  in history[commit_id]["files"]:
                    if "filename" in file_data:
                        # Skip anything in the file_ignores list:
                        ignore = False
                        for exclude in file_ignores:
                            if file_data["filename"].find(exclude) > -1:
                                #print 'Ignoring files with ', exclude, '  - ', file_data["filename"]
                                ignore = True 
                                break
                        if ignore:
                            continue
                        #print "Processing file: ", file_data["filename"]
                        local_file_path = "%s/%s" % (path,file_data["filename"])
    
                        fileName, fileExtension = os.path.splitext(file_data["filename"])
                        # Only process files with a file extension in our list
                        if  ( fileExtension in file_exts):
    
                            # check if file is in database, if not add it and get ID.
                            file_id = _backend.get_file_id(file_data["filename"])
                            if file_id == None:
                                file_id = _backend.add_file_values(file_data["filename"])
                        
                            # Check if this file commit has been added yet or not
                            commit_list  = _backend.get_commit_id(commit_id, file_id[0])
                            if len(commit_list) == 0:
                                
                                bugnumber      = ''
                                is_backout     = 0
                                num_lines      = 1 
                                committer_name = ''
                                reviewer       = ''
                                approver       = ''
                                msg            = ''
                                if os.path.isfile(local_file_path):
                                    num_lines = sum(1 for line in open(local_file_path))
                                else:
                                    # File not found - skip it.
                                    #print 'File Not Found in current repository - SKIPPING %s' % local_file_path
                                    continue
                                if num_lines == 0:
                                   num_lines = 1
                                if ("bug" in history[commit_id].keys()):
                                    bugnumber = history[commit_id]["bug"]
                                if ("is_backout" in history[commit_id].keys()):
                                    if history[commit_id]["is_backout"]:
                                        is_backout  = 1
                                if ("approver" in history[commit_id].keys()):
                                    approver = history[commit_id]["approver"]
                                if ("reviewer" in history[commit_id].keys()):
                                    reviewer = history[commit_id]["reviewer"]
                                if ("committer_name" in history[commit_id].keys()):
                                    committer_name = history[commit_id]["committer_name"]
                                if ("msg" in history[commit_id].keys()):
                                    msg = history[commit_id]["msg"]
            
                                delta = file_data["added"] +  file_data["removed"]
                                percent_change = float("{0:.2f}".format((float(delta) /  float(num_lines)) * 100))
                                is_regression = check_keyword(bugnumber,'regression')
                                _backend.add_change_values(file_id[0], rel_id,  
                                                           delta, num_lines, percent_change,  
                                                           bugnumber, commit_id, is_backout, committer_name, reviewer, approver, msg, is_regression)
                            #else:
                            #    print 'Commit ID: ', commit_id, ' Already Processed - SKIPPING'
            
                        #else:
                        #    print 'Ignoring file with file extension ', fileExtension
            process_release(rel_id)

def check_keyword(bugnumber, keyword):

    url = 'https://bugzilla.mozilla.org/rest/bug/%s?include_fields=id,keywords' % bugnumber
    try:
        response = urllib2.urlopen(url).read()
    except urllib2.HTTPError, e:
        print('HTTPError = ' + str(e.code))
        return
    except urllib2.URLError, e:
        print('URLError = ' + str(e.reason))
        return
    except httplib.HTTPException, e:
        print('HTTPException')
        return
    data = json.loads(response)
        
    if keyword in data['bugs'][0]['keywords']:
        print '%s REGRESSION' % bugnumber
        return True
    else:
        return False


def process_release(release_id):
    global _backend

    print 'Processing Release :', release_id
    # Calculate average change per release for each file
    all_files = _backend.get_files_per_release(release_id)
    regression_list =  {}
    for file in all_files:
        file_id = file[0]
        total_delta = 0
        total_lines = 1
        bugs = ''
        backout_count = 0
        committers = ''
        reviewers = ''
        approvers = '' 
        msgs      = ''
        regression_count = 0
        author_count   = 0
        bug_count      = 0

        #print 'Processing File ID:', file_id
        file_commits = _backend.get_changes_by_file_release(file_id, release_id)
        # 0 file_id, 1 total_lines, 2 delta, 3 percent_change, 4 commit_id, 5 bug, 6 is_backout, 7 committer_name, 8 reviewer, 9 approver, 10 msg, 11 is_regression
        total_commits = 0
        for row in file_commits:
            total_commits += 1
            total_lines    = row[1]
            total_delta   += row[2]
            bug            = row[5]
            is_backout     = row[6]
            is_regression  = row[11]

            if len(bug) > 0:
                # ignore duplicate bugs
                if bugs.find(bug) < 0:
                    bugs += '%s,' % bug
                    bug_count += 1
                    if is_regression == 1:
                        regression_count += 1
            if is_backout:
                backout_count += 1 
            if committers.find(row[7]) <0:
                committers += '%s,' % row[7]
                author_count += 1
            if reviewers.find(row[8]) <0:
                reviewers  += '%s,' % row[8]
            if approvers.find(row[9]) <0:
                approvers  += '%s,' % row[9]
            msgs += '%s) %s ' % (total_commits, re.sub(r'[^a-zA-Z0-9= ]', '',row[10]) )

        total_percent = float("{0:.2f}".format((float(total_delta) / float(total_lines)) * 100))
        summary = _backend.get_summary_data(release_id, file_id)
        if summary == []:
            _backend.add_summary_data(release_id, file_id, total_percent, bugs, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count)
        else:
            _backend.update_summary_data(release_id, file_id, total_percent, bugs, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count )



def process_data():
    global _backend
    # Calculate total average change per file
    all_files = _backend.get_file_ids()
    all_releases = _backend.get_release_ids()
    for file in  all_files:
        file_id = file[0]
        summary = _backend.get_all_summary_data(file_id)
        change_list = []
        file_data = dict()

        for row in summary:
            release_id     = row[0]
            file_id        = row[1]
            percent_change = row[2]

            file_data[release_id] = percent_change
             
        # Fill in data for missing releases
        for release in all_releases:
            release_id = release[0]
            if release_id in file_data.keys():
                change_list.append(file_data[release_id])
            else:
                # File was not changed for this release
                change_list.append(0)
        # Calculate rate of change
        mean,stdev = meanstdv(change_list) 
        mean  = float("{0:.2f}".format(mean))
        stdev = float("{0:.2f}".format(stdev))
        _backend.update_avg_change(file_id, mean, stdev)

#        if len(change_list) == 0:
#            print 'Change List is NULL'
#        else:
#            print 'FILE ID:', file_id, ' STDV:', meanstdv(change_list), ' MAX CHANGE:', max(change_list) 
#            if ( max(change_list)  > (mean + stdev) ):
#                for rel_id,change_rate in file_data.items():
#                    if change_rate == max(change_list):
#                        print 'File ID:', file_id, ' High Change Rate ', change_rate,  ' in Release ', rel_id 

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
