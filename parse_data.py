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
from table_parser import TableParser

# Branchlist format is:
# release-name /path/to/branchsource
# Example:
# nightly-36 /Users/mschifer/mozilla-central
# aurora-36 /Users/mschifer/mozilla-aurora
# beta-36 /Users/mschifer/mozilla-beta
# release-36 /Users/mschifer/mozilla-release


branch_list = ""

# Process each json file in the branch list 
# in proper order (nightly, aurora, beta, release)
# Add each commit to the metrics_changes and update
# file and release tables as required for new entries
###
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
        pprint.pprint( order )
        for release, path in order.items():
            fp = open('%s.json' % release, "r")

            # Make sure we have the release in our list and assigned a release id
            rels =  _backend.get_release_id(release)
            if len(rels) == 0:
                print 'Release Not Found: Adding Release'
                branch,release_number = release.split('-',1)
                rels  =   _backend.add_release_values(release, release_number)
                rel_id = rels[0][0]
            else:
                rel_id = rels[0][0]
                branch,release_number = release.split('-',1)
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
                                bugdata = get_bug_fields(bugnumber, ['keywords','creation_time','cf_last_resolved'])
                                print 'Bug : %s' % bugnumber
                                pprint.pprint(bugdata)
                                if bugdata == None:
                                    print 'No Bug Found'
                                    is_regression = False
                                    found = '2012-01-01'
                                    fixed = '2012-01-01'
                                    bugnumber = 0
                                else:
                                    if 'regression' in bugdata['keywords']:
                                        print '%s REGRESSION' % bugnumber
                                        is_regression = True
                                    else:
                                        is_regression = False
    
                                    foundtime  = bugdata['creation_time']
                                    if foundtime == None:
                                        found  = ""
                                    else:
                                        found, time  = foundtime.split('T',1)
    
                                    fixedtime  = bugdata['cf_last_resolved']
                                    if fixedtime == None:
                                        fixed  = ""
                                    else:
                                        fixed, time  = fixedtime.split('T',1)

                                _backend.add_change_values(file_id[0], rel_id, delta, num_lines, percent_change, bugnumber, commit_id, is_backout, committer_name, reviewer, approver, msg, is_regression, found, fixed)
                                if bugnumber >  0:
                                    update_bug_table(bugnumber)

                            #else:
                            #    print 'Commit ID: ', commit_id, ' Already Processed - SKIPPING'
            
                        #else:
                        #    print 'Ignoring file with file extension ', fileExtension
            process_release(rel_id)
            process_el_search_bugs(release_number)
    process_el_search_bugs('trunk')
    process_el_search_bugs('unspecified')

# *** DEPRICATED ***
# Get a specific field value from Bugzilla 
# for a given Bug number
###
def get_bug_field(bugnumber, field_name):
    rdata = {}
    data = get_bug_data(bugnumber, [field_name])
    if data == None:
        return None
    for f_name in field_names:
        if f_name in data['bugs'][0]:
            print 'DATA FOR %s: %s' % (f_name, data['bugs'][0][f_name])
            rdata[f_name] = data['bugs'][0][f_name]
        else:
            rdata[f_name] = None
    return rdata

# Return a simplified dictionary of Buzilla bug data 
# for a given List of field
###
def get_bug_fields(bugnumber, field_names):
    rdata = {}
    data = get_bug_data(bugnumber, field_names)
    if data == None:
        return None
    for f_name in field_names:
        if f_name in data['bugs'][0]:
            print 'DATA FOR %s: %s' % (f_name, data['bugs'][0][f_name])
            rdata[f_name] = data['bugs'][0][f_name]
        else:
            rdata[f_name] = None
    return rdata

# Query the Bugzilla REST API to 
# get individual bug information
# fields requested must be in a list object
###
def get_bug_data(bugnumber,field_names):

    print 'Getting Bug Number: %s' % bugnumber
    field_list = ""
    if bugnumber == "":
       return None
    for f_name in field_names:
        field_list = ','.join(field_names)
    url = 'https://bugzilla.mozilla.org/rest/bug/%s?include_fields=id,%s' % (bugnumber,field_list)
    print url
    reponse = None
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
    return json.loads(response)

# Calculate the average rate of change for each file 
# for a given release and update the metrics_summary table
###
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
        print 'WHAT DID I GET HERE!!!'
        pprint.pprint(summary)
        print '----------------------'
        if summary == [] or summary == None:
            _backend.add_summary_data(release_id, file_id, total_percent, bugs, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count)
        else:
            _backend.update_summary_data(release_id, file_id, total_percent, bugs, backout_count, committers, reviewers, approvers, msgs, total_commits, bug_count, regression_count, author_count )


# Determine the life time average rate of change and the standard deviation
# per release for each file. This is very inefficent and should be looked at 
# to see if there a better way to calcuate this without having to re-do it 
# for each release each time.
###
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

# Update the master metrics_bug table when bugs are fixed
# in a commit.  Add the bug if it is not already in the table
###
def update_bug_table(bugnumber):
    bugdata = _backend.get_regression_bug(bugnumber)
    if len(bugdata) == 0:
        print 'Bug: %s NOT FOUND' % bugnumber
        data = get_bug_data(bugnumber, ['keywords','creation_time','cf_last_resolved','product','component','keywords','version','status'])
        if data == None:
            release_id = _backend.get_release_id('release-0')
            _backend.add_bug(bugnumber, 'unspecified','2012-01-01', '2012-01-01', 'unknown', 'resolved', 'unknown', 0, release_id[0][0])
            return
        #print data['bugs'][0]['keywords']
        #print data['bugs'][0]['creation_time']
        #print data['bugs'][0]['cf_last_resolved']

        version_str = data['bugs'][0]['version']
        status  = data['bugs'][0]['status']
        product = data['bugs'][0]['product']
        component = data['bugs'][0]['component']

        found,time  = data['bugs'][0]['creation_time'].split('T',1)
        if data['bugs'][0]['cf_last_resolved'] == None:
            fixed = ''
        else:
            fixed,time  = data['bugs'][0]['cf_last_resolved'].split('T',1)

        is_regression = 0
        if data['bugs'][0]['keywords'] <> [] and 'regression' in data['bugs'][0]['keywords']:
            #print '%s REGRESSION' % bug
            is_regression = 1
        if 'avenir' in version_str.lower() or 'seamonkey' in  version_str.lower():
            print 'Skipping Bug: %s - version is %s' % (bugnumber, version_str)
            return
        release_id = find_release_id(version_str, found)
        #print "%s %s %s %s %s %s %s %s %s" % ((bugnumber, version_str, found, fixed, product, status, component, is_regression, release_id))

        _backend.add_bug(bugnumber, version_str, found, fixed, product, status, component, is_regression, release_id)


# Create the Elastic Search Query to pull in all
# Bugs that do NOT have a negative (invalid) resolution
# and add them to the metrics_bugs master table
# Note: Need to remove redudent data from metrics_changes table
###
def process_el_search_bugs(version):

    # default start date 
    str = '2013-05-13'
    format = '%Y-%m-%d'
    current_milli_time = lambda: int(round(time.time() * 1000))

    start_date = int(time.mktime(time.strptime(str, format))) * 1000
    today_date = current_milli_time()

    product_list = [ 
        
        "firefox", 
        "firefox os", 
        "core",
        "firefox for android",
        "firefox",
        "nss",
        "toolkit",
        "firefox health report",
        "boot2gecko",
        "webtools",
        "android background services",
        "loop"
         ]
    exclude_status = [
        "invalid",
        "worksforme",
        "wontfix",
        "duplicate",
        "expired",
        "support"
        ]
    print 'Processing Elastic Search Bug Data'
    print version
    print start_date
    print today_date
    print product_list
    print exclude_status

    #query = { "query": {"filtered": { "query": {"match_all":{}}, "filter": { "and": [ {"or": [ {"missing": {"field": "resolution"}}, { "not":{"terms": {"resolution": ["invalid", "worksforme", "wontfix", "duplicate", "expired", "support"]}}} ]},{"terms":{"product":["firefox", "firefox os", "core", "firefox for android", "firefox", "nss", "toolkit", "firefox health report", "boot2gecko", "webtools", "android background services", "loop"] }}, {"term": {"version": version}}, {"range": {"created_ts": {"gte": start_date}}}, {"range": {"expires_on": {"gte": today_date }}} ] } }}, "from": 0, "fields": ["product", "bug_id", "bug_status", "resolution", "created_ts", "cf_last_resolved", "version", "keywords", "component"], "size": 10000, "sort": [], "facets": {} }

    query = { "query": {"filtered": { "query": {"match_all":{}}, "filter": { "and": [ {"or": [ {"missing": {"field": "resolution"}}, { "not":{"terms": {"resolution": exclude_status }}} ]},{"terms":{"product": product_list }}, {"term": {"version": version}}, {"range": {"created_ts": {"gte": start_date}}}, {"range": {"expires_on": {"gte": today_date }}} ] } }}, "from": 0, "fields": ["product", "bug_id", "bug_status", "resolution", "created_ts", "cf_last_resolved", "version", "keywords", "component"], "size": 10000, "sort": [], "facets": {} }

    get_el_search_data(query)


# Use the Elastic Search API to query the 
# Bugzilla Elastic Search Database
# Requires a json query to be passed in
# and will return a json result file
###
def get_el_search_data(json_query):

    url = "https://esfrontline.bugzilla.mozilla.org:443/public_bugs/bug_version/_search"

    data = json.dumps(json_query)

    clen = len(data)
    req = urllib2.Request(url, data, {'Content-Type': 'application/json', 'Content-Length': clen})
    f = urllib2.urlopen(req)
    response = f.read()
    f.close()

    bugdata = json.loads(response)
    for bug in bugdata['hits']['hits']:
        bugnumber  = bug['fields']['bug_id']
        status  = bug['fields']['bug_status']
        version_str  = bug['fields']['version']
        found = time.strftime('%Y-%m-%d', time.localtime(bug['fields']['created_ts']/1000))
        component = bug['fields']['component']
        product = bug['fields']['product']
        is_regression = 0
        if 'keywords' in bug['fields'].keys():
            if 'regression' in bug['fields']['keywords']:
                is_regression = 1

        if ('cf_last_resolved' in bug['fields'].keys()):
            fixed =   time.strftime('%Y-%m-%d', time.localtime( bug['fields']['cf_last_resolved']/1000))
        else:
            fixed = 0
        release_id = find_release_id(version_str, found) 
        if  _backend.get_regression_bug(bugnumber) == []:
            print 'Adding Bug : %s' % bugnumber
            _backend.add_bug(bugnumber, version_str, found, fixed, product, status, component, is_regression, release_id)
        else:
            print 'Updateing existing Bug : %s %s %s %s %s' % (bugnumber, fixed, status, is_regression, release_id)
            _backend.update_bug(bugnumber, fixed, status, is_regression, release_id)

# Given a found date and a version string
# try and determine which branch of the release
# it was filed against.
# Assume highest level branch available
# on that date.  This is best guess
###
def find_release_id(version_str, found):

        # Version strings have a lot of variation in them over time 
        # Try to normalize them into something sane

        print "LOOK FOR %s %s" % (version_str, found)
        assume_nightly_version_strings = ['other branch', 'other', 'trunk']
        assume_release_version_strings = ['unspecified', '---']
        if 'trunk' in version_str.lower() or 'unspecified' in version_str.lower() or 'other' in version_str.lower() or '---' in version_str:
            version = version_str.lower()
        else:
            version = version_str.lower().replace('firefox ','')
            if '.' in version:
                version, junk = version.split('.',1)
            version = int(version.replace(' branch',''))
        max_versions = _backend.get_max_versions(found)
        pprint.pprint(max_versions)
        # nightly, aurora, beta, release

        release_name = 'release-0'
        if max_versions == []:
            print 'Max Release Not Found - too old'
        else:
            print 'MAX Versions: N: %s A: %s B: %s R: %s' % (max_versions[0][0], max_versions[0][1], max_versions[0][2], max_versions[0][3])
            if max_versions[0][0] == version:
                release_name = 'nightly-%s' % version
            if max_versions[0][1] == version:
                release_name = 'aurora-%s' % version
            if max_versions[0][2] == version:
                release_name = 'beta-%s' % version
            if max_versions[0][3] == version:
                release_name = 'release-%s' % version
            #if version == 'trunk' or version == 'other branch' or version == 'other':
            if version in assume_nightly_version_strings:
                release_name = 'nightly-%s' % max_versions[0][0]
                version = max_versions[0][0]
            if version in assume_release_version_strings:
                release_name = 'release-%s' % max_versions[0][3]
                version = max_versions[0][3]
        release_id = _backend.get_release_id(release_name)
        if release_id == []:
            print 'RELEASE NOT FOUND'
            release_id =  _backend.add_release_values(release_name, version)
            update_release_schedule(version)
        print 'RELEASE: %s  ID %s' % (release_name, release_id[0][0])
        return release_id[0][0]

# Add entires to the metrics_release_schedule lookup table.
# Assume that if we find a new release number that is higher than
#the current max nighty number that is is a new release and that it starts on 
# the day it was found.  This assumes the script is run nightly.
# This will NOT add older releases 
###
def update_release_schedule(release_number):
    # If release_number is not in metrics_releases AND 
    # metrics_release_schedule.nightly not found.
    # Add days date and release_number, -1, -2, -3 to 
    # metrics_release_scheduleA
    max_nightly = _backend.get_max_nightly()
    if release_number > max_nightly:
        start_date = time.strftime('%Y-%m-%d')
        print 'Updating release schedule with new release'
        print 'Start Date: %s N %s, A %s B %s R %s' % (start_date, release_number, release_number - 1, release_number - 2, release_number - 3)
        _backend.add_release_schedule_entry(start_date, release_number, release_number - 1, release_number - 2, release_number - 3)
    else:
        print 'Release %s already in release_schedule' % (release_number)


def import_release_calendar():
    # Parse the rapid release calendar web page
    url = 'https://wiki.mozilla.org/RapidRelease/Calendar'
    
    req = urllib2.Request(url)
    f = urllib2.urlopen(req)
    tp = TableParser()
    tp.feed(f.read())
    f.close()
    releaseDates = []
    rowcnt = 0
    for row in tp.doc:
        if rowcnt > 1:
            print 'Branch Dates'
            for element in row:
                if element[0][0] == 'quarter ' or element[0][0] == 'merge date':
                    continue
                
                if re.match('^Q',str(element[0][0])):
                    if len(element) >  6:
                        releaseDates.append( { 'startDate':element[1][0].rstrip(),
                                             'nightly':element[2][0].rstrip().replace('Firefox ',''),
                                             'aurora':element[3][0].rstrip().replace('Firefox ',''),
                                             'beta':element[4][0].rstrip().replace('Firefox ',''),
                                             'endDate':element[5][0].rstrip().replace('Firefox ',''),
                                             'release':element[6][0].rstrip().replace('Firefox ','')
                                              } )
                else:
                    if len(element)  > 5:
                        releaseDates.append( { 'startDate':element[0][0].rstrip().replace('Firefox ',''),
                                             'nightly':element[1][0].rstrip().replace('Firefox ',''),
                                             'aurora':element[2][0].rstrip().replace('Firefox ',''),
                                             'beta':element[3][0].rstrip().replace('Firefox ',''),
                                             'endDate':element[4][0].rstrip().replace('Firefox ',''),
                                             'release':element[5][0].rstrip().replace('Firefox ','')
                                             } 
                                            )
        rowcnt += 1
    for schedule in releaseDates:
        try:
            time.strptime(schedule['startDate'], '%Y-%m-%d')
            print 'Valid Start Date: %s' % schedule['startDate']
        except ValueError:
             print 'Invalid Start Date: %s' % schedule['startDate']
             continue
    
        checkit = _backend.get_release_schedule(schedule['startDate'])
        if checkit == None:
            print 'ADD: %s, %s, %s, %s, %s' % (schedule['startDate'], schedule['nightly'], schedule['aurora'], schedule['beta'], schedule['release'])
            _backend.add_release_schedule(schedule['startDate'], schedule['nightly'], schedule['aurora'], schedule['beta'], schedule['release'])

        else:
            print 'ALREADY ADDED: %s, %s, %s, %s, %s' % (schedule['startDate'], schedule['nightly'], schedule['aurora'], schedule['beta'], schedule['release'])
            pprint.pprint(checkit)


def get_phone_book_data(username, password):

    url = 'https://phonebook.mozilla.org/search.php?format=json'

    passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
    print 'USERNAME:%s:' % username
    print 'PASSWORD:%s:' % password
    passman.add_password(None, url, username, password)
    urllib2.install_opener(urllib2.build_opener(urllib2.HTTPBasicAuthHandler(passman)))
    req = urllib2.Request(url)
    f = urllib2.urlopen(req)
    response = f.read()
    data = json.loads(response)

    for person in data:
        #pprint.pprint(person)
        print '----'
        for k in person.keys():
            pprint.pprint('K: %s, V:%s' %(k, person[k]))
        rec = {}
        rec['manager_name'] = ''
        rec['manager_email'] = ''
        for k in {'cn','employeenumber','bugzillaemail','deptname','mail','manager'}:
            if k in person.keys():
                if k == 'deptname':
                   rec['manager_name'] = person['deptname'][person['deptname'].find("(")+1:person['deptname'].find(")")]
                if k == 'manager':
                    if  person[k] == None:
                        rec['manager_email'] = 'None'
                    else:
                       rec['manager_email'] = person['manager']['dn'][person['manager']['dn'].find("=")+1:person['manager']['dn'].find(",")]
                       #rec['manager_email'] = person[k]['dn'].find("=")+1:person[k]['dn'].find(","))
                rec[k] = person[k]
            else:
                rec[k] = ''
        print 'Bugzilla EM   : ', rec['bugzillaemail']
        print 'Department    : ', rec['deptname']
        print 'E-Mail        : ', rec['mail']
        print 'Manager Name  : ', rec['manager_name']
        print 'Manager eMail : ', rec['manager_email']
        print 'Emp Name cn   : ', rec['cn']
        print 
        _backend.add_person(rec['cn'],rec['bugzillaemail'],rec['mail'],rec['manager_email'],rec['deptname'])
        

       
def build_teams():

    #GET_ALL_COMMITTERS = 'SELECT name, bzemail, email, manager_email, department FROM metrics_people'

    managers = {}
    all_people = _backend.get_all_people()
    for person in all_people:
        persons_name    = person[0]
        bzemail         = person[1]
        email           = person[2]
        manager_email   = person[3]
        department      = person[4]
        if manager_email not in managers.keys():
            managers[manager_email] = []

    for manager in managers.keys():
        print '%s' %  manager
        for employee in managers[manager]:
            #pprint.pprint(employee)
            print '\t %s' % str(employee)
        manager_id = _backend.get_people_id(manager)
        pprint.pprint(manager_id)
        if manager_id == None:
           manager_id = [0]
        
        _backend.update_manager_id(manager_id[0], manager)
    


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

    init_data = False
    init_args = False

    parser = argparse.ArgumentParser()
    run_group = parser.add_argument_group('Process Data')
    run_group.add_argument("-b", action='store', dest="branch_list",  help="File containing branch list")

    init_group = parser.add_argument_group('Init System')
    init_group.add_argument("-i", action='store_true', dest="init", help="Initilize database on first run ")
    init_group.add_argument("-u", action='store', dest="username",  help="LDAP username for phonebook access")
    args = parser.parse_args()

    if args.init or args.username:
        if args.init and args.username:
            init_args = True
        else:
            print '\n'
            if not args.username:
                 print 'ERROR: -u username option required if -i or -p specified.'
                 init_args = False
            if not args.init:
                 print 'ERROR: -i opion required to (re)initialize database.'
                 init_args = False
        if not init_args:
            print '\n'
            parser.print_help()
            sys,exit()
             
    if init_args:
        print '**********************************************************'
        print 'WARNING: -i will delete all data and recreate the database'
        print '            ALL    DATA    WILL    BE    LOST'
        print '**********************************************************'
        confirm = raw_input('Do you wish to reset the database (YES)/no? ')
        if confirm == 'YES':
            init_data = True
        else:
            print 'You typed "%s", exiting' % confirm
            parser.print_help()
            sys,exit()

    _backend = SQLiteBackend()

    if args.init:
        if args.username:
            password = raw_input('Enter LDAP password: ')
            print 'Initializing database'
            _backend.init_data()
            get_phone_book_data(args.username, password)
            build_teams()
            import_release_calendar()
        else:
            print 'LDAP username (-u) and password (-p) required to import phone book integration to complete init process'
            sys,exit()
    if args.branch_list:
        print 'Importting data'
        parse_data()
        process_data()
    else:
        print 'No branch list file spefifed. Not importing data'
