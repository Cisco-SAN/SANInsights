import requests, getpass, shutil, logging, warnings, csv, gc
import random, sys, time, os, getopt
from collections import defaultdict
import tarfile, gzip
import multiprocessing
from elasticsearch import Elasticsearch, helpers, RequestsHttpConnection
import http.client
from datetime import datetime

http.client._MAXLINE = 1000000000 # https://stackoverflow.com/questions/63157046/python-http-request-exception-linetoolong

SWIP = None
PORT = None
VSAN = None
INIT = None
TARGET = None
LUN = None
NSID = None

REMOTE = None
VERBOSE = None
PASSWORD = None
USERNAME = None 
REMOTE_IP = None
REMOTE_PATH = None

EST_FILE_SIZE_PER_DOC = 320 #bytes
EST_TIME_PER_DOC = 0.001 #seconds

logfile_name = 'si_fetch_data.log'
logging.basicConfig(filename = logfile_name, filemode='w', format='%(asctime)s - %(message)s', level=logging.WARNING)

warnings.filterwarnings("ignore")

concerned_keys = ['sid', 'did', 'lun', 'port', 'swName', 'timestamp',
                  'arECT', 'awECT', 'nrDAL', 'nrECT', 'nwECT', 'nrIOP',
                  'nrIOS', 'nwDAL', 'nwIOP', 'nwIOS', 'arDAL', 'awDAL', 'arThru',
                  'awThru', 'rAbo', 'wAbo', 'rFail', 'wFail', 'rIOa', 'rIObw', 'rIOf',
                  'raIO', 'wIOa', 'wIObw', 'wIOf', 'waIO', 'rECTst', 'wECTst', 'rECTbl',
                  'wECTbl', 'rECTDev', 'wECTDev']

pmdb_sanportratedata_concerned_keys = ['ifIndex', 'tx', 'rx', 'inError', 'txUsage', 'ifName', 'type',
                                        'outDiscard', 'wwn', 'inDiscard', 'rxUsage', 'outError', 'switchName', 'timestamp']

match_all = {
    "track_total_hits": "true",
    "query": {
        "match_all": {}
    }
}

try:
    es = Elasticsearch(['localhost:9200'], timeout = 10000)
except Exception as e:
    print('Connection to Elastcisearch python client failed due to err : ', e)

def hex2dec(hex_string):
    hex_string = hex_string.partition('x')[2]
    base = 16
    
    return int(hex_string, base)
    
def get_IP_to_switchname_mapping():
    switch_name_dict = dict()
    resp = helpers.scan(es, index = 'dcmdb-switch', query = match_all)
    data = [x['_source'] for x in resp]
    for item in data:
        if item['sysName'] not in switch_name_dict.keys():
            switch_name_dict[item['sysName']] = 'switch' + str(len(switch_name_dict.keys()) + 1)
    
    return switch_name_dict

def show_dot_progress(data, freq):
    if len(data) % freq == 0:
        sys.stdout.write('.')
        sys.stdout.flush()

def port_sanity_check(port_list):
    retval = True
    for port in port_list:
        if '/' not in port:
            print('Wrong input format for port !. Please refer to --help for examples.')
            retval = False
            return retval
        port = port.replace('/', '')
        if port.isnumeric():
            print('Wrong input format for port !. Please refer to --help for examples.')
            retval = False
            return retval
    return retval

def rollup_index_sanity_check():
    null_check_query = {
        "query": {
            "bool": {
                "must_not": [
                    {
                        "exists": {
                            "field": "vsan"
                        }
                    }
                ]
            }
        }
    }

    for index in es.indices.get('*'):
        if 'san_fc_flows_rollup_' in index:
            index_to_be_checked = index
            break
    resp = helpers.scan(es, index = index_to_be_checked, query = null_check_query)
    count = 0
    for doc in resp:
        count += 1
    
    #print('\nVerifying index {} for null values for VSAN field. Null values found = {}'.format(index_to_be_checked, count))
    if count > 0:
        # print('WARNING : VSAN field missing in rollup indices. Ignoring VSAN filter for rollup indices.')
        return False
    else:
        # print('Sanity check passed.')
        return True


def get_switchname_from_swip(ip):
    #print('\nSearching for the switchname for the given switch IP ...')
    logging.warning('Searching for the switchname for the given switch IP ...')
    # Getting switchname from swIP
    swip_switchname_query = {
        "query": {
            "bool": {
                 "must": [
                     { "term": {"ipAddress": ip}}
                 ]
             }
        }
    }
    logging.warning('swip_switchname_query = %s', swip_switchname_query)

    resp = helpers.scan(es, index='dcmdb-switch', query=swip_switchname_query)
    query_data = [doc['_source'] for doc in resp]
    count = len(query_data)
    #print('Doc count : ', count)
    
    if count == 0:
        print('Switch IP not found, please try again with different input.')
        logging.error('Switch IP not found, please try again with different input.')
        sys.exit()
    elif count == 1:
        switchname = query_data[0]['sysName']
        print('Switch found : ', switchname)
        logging.warning('Switch found : %s', switchname)
        return switchname
    else:
        print('WARNING : Multiple switches with same IP found !')
        logging.warning('Multiple switches with same IP found !')
        for data in query_data:
            logging.warning(data['sysName'])
        logging.warning('Picking the first switch ...')    
        switchname = query_data[0]['sysName']
        print('Switch found : ', switchname)
        return switchname
    
def write_to_csv_file(filename, data):
    total, used, free = shutil.disk_usage('/')
    # logging.warning('Estimated disk space required : %s', EST_FILE_SIZE_PER_DOC * len(data))
    if free < EST_FILE_SIZE_PER_DOC * len(data):
        logging.error('Not enough disk space to write more data. Exitting ...')
        print('Not enough disk space to write more data. Exitting ...')
        sys.exit()
    # else:
    #     logging.warning('Enough free space available. Current free space : %s', free)

    if not os.path.exists(filename):
        with open(filename, 'w', encoding='utf8', newline='') as output_file:
            # extrasaction - Reference : http://www.lucainvernizzi.net/blog/2015/08/03/8x-speed-up-for-python-s-csv-dictwriter/
            fc = csv.DictWriter(output_file, fieldnames = data[0].keys(), extrasaction = 'ignore') # We do not want to write the header again
            fc.writeheader()
            fc.writerows(data)
    else:
        with open(filename, 'a', encoding='utf8', newline='') as output_file:
            # extrasaction - Reference : http://www.lucainvernizzi.net/blog/2015/08/03/8x-speed-up-for-python-s-csv-dictwriter/
            fc = csv.DictWriter(output_file, fieldnames = data[0].keys(), extrasaction = 'ignore') # We do not want to write the header again
            fc.writerows(data)


def generate_report(switch = None):
    print('\nGenerating report, please wait for some time ...')
    distinct_itl_query = {
      "size": "0",
      "aggs": {
        "distinct_itls": {
          "cardinality": {
            "field": "itl"
          }
        }
      }
    }

    distinct_itcn_query = {
      "size": "0",
      "aggs": {
        "distinct_itcns": {
          "cardinality": {
            "field": "itcn"
          }
        }
      }
    }

    report = defaultdict(list)
    for index in es.indices.get('*'):
        if 'san_ect_baseline' in index :
            switchname = index.split('_')[3]
            resp = es.search(index = index, body = distinct_itl_query)
            report[switchname].append(resp['aggregations']['distinct_itls']['value'])

        if 'san_nvme_ect_baseline' in index :
            switchname = index.split('_')[4]
            resp = es.search(index = index, body = distinct_itcn_query)
            report[switchname].append(resp['aggregations']['distinct_itcns']['value'])

    final_report = dict()
    for key, value in report.items():
        final_report[key] = max(value)

    if switch:
        if switch in final_report.keys():
            return final_report[switch]
        else:
            print('No data present for specifed switch. Exiting ...')
            sys.exit()
    else:
        print('Switchname', ' '*(50-len('Switchname')), 'ITL/ITCN Count')
        for key, value in final_report.items():
            print(key, ' '*(50-len(key)), value)


def generate_zip_object(file_list, zipfile_name):
    for filename in file_list:
        with open(filename, 'rb') as f_in:
            with gzip.open(filename + '.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(filename)
        logging.warning('Deleting file %s', filename)
    
    # Moving all the zipped csv files to new directory.
    os.mkdir(zipfile_name)
    for filename in file_list:
        shutil.move(src = filename + '.gz', dst = zipfile_name)

    # with tarfile.open(zipfile_name + '.tar.gz', "w:gz") as tar:
    #     for file in file_list:
    #         tar.add(file)
    print('{} generated successfully.'.format(zipfile_name))

def multiprocessing_func(index, query, file_list, return_list, mapping_dict, VERBOSE, type, SWIP, REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH):
    # Fetching data for a specified input
    global concerned_keys

    tmp = index.split('_')
    if type in ['fc_flows']:
        tmp[2] = mapping_dict[get_switchname_from_swip(SWIP)]
    elif type in ['fc_nvme_flows', 'san_ect_baseline']:
        tmp[3] = mapping_dict[get_switchname_from_swip(SWIP)]
    elif type in ['san_nvme_ect_baseline']:
        tmp[4] = mapping_dict[get_switchname_from_swip(SWIP)]

    tmp = '_'.join(x for x in tmp)
    filename = tmp + '_fetched_data.csv'

    # Renaming filenames for better readability
    if type == 'fc_flows':
        filename = filename.replace('fc_flows', 'fc_scsi_rawdata')
    elif type == 'fc_nvme_flows':
        filename = filename.replace('fc_nvme_flows', 'fc_nvme_rawdata')
    elif type == 'san_ect_baseline':
        filename = filename.replace('san_ect_baseline', 'fc_scsi_enrichdata')
    elif type == 'san_nvme_ect_baseline':
        filename = filename.replace('san_nvme_ect_baseline', 'fc_nvme_enrichdata')
    elif type == 'san_fc_flows_rollup':
        filename = filename.replace('san_fc_flows_rollup', 'fc_scsi_1hr_rollup') 
    elif type == 'san_nvme_rollup':
        filename = filename.replace('san_nvme_rollup', 'fc_nvme_1hr_rollup') 

    resp = helpers.scan(es, index = index, query = query, raise_on_error = False, clear_scroll = False)
    data = []
    sub_total_doc = 0
    try : 
        for raw_doc in resp:
            if type == 'fc_flows':
                doc = raw_doc['_source']
            else:
                doc = {x: raw_doc['_source'][x] for x in concerned_keys}
            
            # fc_flows indices do not contain swName field. They have switchname stored in swIP field.
            if type == 'fc_flows':
                doc['swIP'] = mapping_dict[doc['swIP']]
            else:
                doc['swName'] = mapping_dict[doc['swName']]


            data.append(doc)

            # Keep writing chuks of 100000 docs to csv and free runtime memory so the process does not killed due to OoM Error.
            if len(data) == 100000:
                # This appends the data to the existing csv file
                # logging.warning('Appending a chunk of %s docs to %s', len(data), filename)
                show_dot_progress(data, 100000)
                write_to_csv_file(filename, data)
                sub_total_doc += len(data)
                data.clear()
                continue
    except Exception as e:
        logging.error('Got an exception with error: %s', e)

    # Writing the last chunk of data to csv file.
    if len(data):
        # This appends the data to the existing csv file
        # logging.warning('Appending a chunk of %s docs to %s', len(data), filename)
        write_to_csv_file(filename, data)
        sub_total_doc += len(data)
        data.clear()

    if sub_total_doc:
        if VERBOSE :
            print('\nNumber of relevant docs found in index {} : {}'.format(index, sub_total_doc))
            print('Data saved to file : ', filename)

        if REMOTE:
            with open(filename, 'rb') as f_in:
                with gzip.open(filename + '.gz', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            copy_cmd = 'sshpass -p {} scp -o StrictHostKeyChecking=no {} {}@{}:{}'.format(PASSWORD, filename + '.gz', USERNAME, REMOTE_IP, REMOTE_PATH)
            os.system(copy_cmd)
            logging.warning('Copying the generated csv file to remote location ...')
            logging.warning('Deleting file {} from local workspace ...'.format(filename))
            os.remove(filename)
            os.remove(filename + '.gz')
            if VERBOSE :
                print('\n{} successfully copied to remote location and deleted from local space.'.format(filename))
        else:
            file_list.append(filename)
        
        return_list.append(sub_total_doc)
    else:
        return_list.append(0)
        if VERBOSE :
            print('No relevant docs found in index : {}\n'.format(index))

def main(argv):
    global SWIP, PORT, VSAN, INIT, TARGET, LUN, NSID
    global VERBOSE, REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH
    global concerned_keys

    mapping_dict = get_IP_to_switchname_mapping()

    # rawdata is set to False be default.
    index_to_fetch = {"scsi" : True, "nvme" : True, "rawdata" : False, "enrichdata" : True, "rollupdata" : True, "sanportrate" : True, "slowdraincounter" : True}

    query_internals_for_fc_flows = []
    query_internals_for_san_ect_baseline = []
    query_internals_for_rollups = []

    MP_USED = False
    MAX_PROCS = 1
    generated_csv_filelist = []
    manager = multiprocessing.Manager() # Shared resource between different processes
    pmdb_sanportratedata = False
    pmdb_slowdraincounterdata = False
    
    try:
        opts, args = getopt.getopt(argv, "hs:e:i:p:v:i:t:l:n:c:rv", ['help', 'swip=', 'exclude=', 'include=', 'port=', 'vsan=', 'init=', 'target=', 'lun=', 'nsid=', 'cores=', 'remote', 'verbose',])
        for opt, arg in opts:
            if opt in ("--help"):
                print ('''
                Usage: python3 si_fetch_data.py 
                --swip <switch_ip> 
                [--exclude <comma separated keywords : scsi, nvme, enrichdata, rollup, sanportrate, slowdraincounter]
                [--include] <comma separted keywords : rawdata]
                [--vsan <vsan>]
                [--port <port>]
                [--init <comma separated Initiator FCIDs>]
                [--target <comma separated Target FCIDs>]
                [--lun <comma separated LUNs for SCSI>]
                [--nsid <comma separated NSIDs for NVMe>]
                [--cores <number of cores>]
                [--remote]                  
                [--verbose]
                ''')
                sys.exit()
            elif opt in ("--swip"):
                SWIP = arg
                query_internals_for_fc_flows.append({"term": {"swIP": get_switchname_from_swip(SWIP)}})
                query_internals_for_san_ect_baseline.append({ "term": {"swIP": SWIP}})
                query_internals_for_rollups.append({ "term": {"swIP": SWIP}})
            elif opt in ("--exclude"):
                excludes = [x for x in arg.split(',')]
                for item in excludes:
                    if item in index_to_fetch.keys():
                        index_to_fetch[item] = False
                    else:
                        print('Wrong arg passed to excludes. Please check --help !')
                        sys.exit()
            elif opt in ("--include"):
                includes = [x for x in arg.split(',')]
                for item in includes:
                    if item in index_to_fetch.keys():
                        index_to_fetch[item] = True
                    else:
                        print('Wrong arg passed to includes. Please check --help !')
                        sys.exit()
            elif opt in ("--port"):
                PORT = [x for x in arg.split(',')]
                if not port_sanity_check(PORT):
                    sys.exit()
                query_internals_for_fc_flows.append({ "terms": {"port": PORT}})
                query_internals_for_san_ect_baseline.append({ "terms": {"port": PORT}})
                query_internals_for_rollups.append({ "terms": {"port": PORT}})
            elif opt in ("--vsan"):
                VSAN = [int(x) for x in arg.split(',')]
                query_internals_for_fc_flows.append({ "terms": {"vsan": VSAN}})
                query_internals_for_san_ect_baseline.append({ "terms": {"vsan": VSAN}})
                if rollup_index_sanity_check():
                    query_internals_for_rollups.append({ "terms": {"vsan": VSAN}})
            elif opt in ("--init"):
                INIT = [hex2dec(x) for x in arg.split(',')]
                query_internals_for_fc_flows.append({ "terms": {"sid": INIT}})
                query_internals_for_san_ect_baseline.append({ "terms": {"sid": INIT}})
                query_internals_for_rollups.append({ "terms": {"sid": INIT}})
            elif opt in ("--target"):
                TARGET = [hex2dec(x) for x in arg.split(',')]
                query_internals_for_fc_flows.append({ "terms": {"did": TARGET}})
                query_internals_for_san_ect_baseline.append({ "terms": {"did": TARGET}})
                query_internals_for_rollups.append({ "terms": {"did": TARGET}})
            elif opt in ("--lun"):
                LUN = [x for x in arg.split(',')]
                query_internals_for_fc_flows.append({ "terms": {"lun": LUN}})
                query_internals_for_san_ect_baseline.append({ "terms": {"lun": LUN}})
                query_internals_for_rollups.append({ "terms": {"lun": LUN}})
            elif opt in ("--nsid"):
                NSID = [x for x in arg.split(',')]
                query_internals_for_fc_flows.append({ "terms": {"ni": NSID}})
                query_internals_for_san_ect_baseline.append({ "terms": {"ni": NSID}})
                query_internals_for_rollups.append({ "terms": {"ni": NSID}})
            elif opt in ("--cores"):
                MP_USED = True
                MAX_PROCS = int(arg)
                if MAX_PROCS == 0:
                    print('Cannot set number of cores to 0 !!! Setting it to 1.')
                    MAX_PROCS = 1
                elif MAX_PROCS > 32:
                    print('Cannot set cores to {}. Setting it to maximum cores allowed (ie. 32).'.format(MAX_PROCS))
                    MAX_PROCS = 32
            elif opt in ("--remote"):
                REMOTE = True
            elif opt in ("--verbose"):
                VERBOSE = True

    except getopt.error as err:
        print (str(err))
        sys.exit(2)

    if SWIP == None:
        print('Switch IP is a required argument. Please provide a switch ip for which you want to extract the data.')
        sys.exit()

    if REMOTE:
        print('Please enter the required info for copying the data to remote location.')
        USERNAME = input('Username : ')
        PASSWORD = getpass.getpass(prompt='Password : ')
        REMOTE_IP = input('Remote IP : ')
        REMOTE_PATH = input('NOTE : Please provide a path to a folder that already exists. \nRemote path : ')
        cmd = "echo 'Test file for verifying ssh credentials' > test.txt"
        os.system(cmd)
        print('Verifying SSH credentials ...')
        if os.path.exists('test_out.txt'):
            os.remove('test_out.txt')
        copy_cmd = 'sshpass -p {} scp -o StrictHostKeyChecking=no test.txt {}@{}:{} >> test_out.txt 2>&1'.format(PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH)
        os.system(copy_cmd)
        with open('test_out.txt', 'r') as file:
            data = file.read()
            if 'Permission denied' in data:
                print('Please check your SSH credentials and try again. Exiting ...')
                sys.exit()
            else:
                print('SSH credentials verified successfully.')

    match_itl_for_fc_flows = {
    "query": {
        "bool": {
            "must": query_internals_for_fc_flows
            }
        }
    }

    match_itl_for_san_ect_baseline = {
    "query": {
        "bool": {
            "must": query_internals_for_san_ect_baseline
            }
        }
    }

    match_itl_for_rollups = {
    "query": {
        "bool": {
            "must": query_internals_for_rollups
            }
        }
    }

    if VERBOSE:
        print('Printing query filters...')
        print('query_internals_for_fc_flows : ', query_internals_for_fc_flows)
        print('query_internals_for_san_ect_baseline : ', query_internals_for_san_ect_baseline)
        print('query_internals_for_rollups : ', query_internals_for_rollups)

    # Generating Report
    # generate_report()
    
    #print(SWIP, PORT, VSAN, INIT, TARGET)
    switchname = get_switchname_from_swip(SWIP)
    num_itls = generate_report(switchname.lower())


    def get_num_docs_query(query_internals):
        num_docs_query = {
        "track_total_hits": "true",
        "query": {
            "bool": {
                "must": query_internals
                }
            }
        }
        return num_docs_query

    fc_flows_doc_count = []
    fc_nvme_flows_doc_count = []
    san_ect_doc_count = []
    san_fc_flows_rollup_doc_count = []
    san_nvme_ect_doc_count = []
    san_nvme_rollup_doc_count = []
    pmdb_sanportratedata_doc_count = 0
    pmdb_slowdraincounterdata_doc_count = 0

    for index in es.indices.get('*'):
        if 'fc_flows_' + switchname.lower() in index:
            resp = es.search(body = get_num_docs_query(query_internals_for_fc_flows), index = index)
            fc_flows_doc_count.append(resp['hits']['total'])
        elif 'san_ect_baseline_' + switchname.lower() in index:
            resp = es.search(body = get_num_docs_query(query_internals_for_san_ect_baseline), index = index)
            san_ect_doc_count.append(resp['hits']['total'])
        elif 'san_fc_flows_rollup_' in index:
            resp = es.search(body = get_num_docs_query(query_internals_for_rollups), index = index)
            san_fc_flows_rollup_doc_count.append(resp['hits']['total'])
            
        elif 'fc_nvme_flows_' + switchname.lower() in index:
            resp = es.search(body = get_num_docs_query(query_internals_for_fc_flows), index = index)
            fc_nvme_flows_doc_count.append(resp['hits']['total'])
        elif 'san_nvme_ect_baseline_' + switchname.lower() in index:
            resp = es.search(body = get_num_docs_query(query_internals_for_san_ect_baseline), index = index)
            san_nvme_ect_doc_count.append(resp['hits']['total'])
        elif 'san_nvme_rollup_' in index:
            resp = es.search(body = get_num_docs_query(query_internals_for_rollups), index = index)
            san_nvme_rollup_doc_count.append(resp['hits']['total'])
        
        elif index == 'pmdb_sanportratedata':
            pmdb_sanportratedata = True
            resp = es.search(body = match_all, index = index)
            pmdb_sanportratedata_doc_count = resp['hits']['total']
            
        elif index == 'pmdb_slowdraincounterdata':
            pmdb_slowdraincounterdata = True
            resp = es.search(body = match_all, index = index)
            pmdb_slowdraincounterdata_doc_count = resp['hits']['total']
    
    if pmdb_slowdraincounterdata:
        print('\nTotal number of SLOWDRAIN_COUNTER_DATA docs : ', pmdb_slowdraincounterdata_doc_count)
    if pmdb_sanportratedata:
        print('\nTotal number of SAN_PORT_RATE_DATA docs : ', pmdb_sanportratedata_doc_count)

    print('\nTotal number of FC_FLOWS docs : ', sum(fc_flows_doc_count))
    print('Total number of SAN_ECT_BASELINE docs : ', sum(san_ect_doc_count))
    print('Total number of SAN_FC_FLOWS_ROLLUP docs : ', sum(san_fc_flows_rollup_doc_count))

    print('\nTotal number of FC_NVME_FLOWS docs : ', sum(fc_nvme_flows_doc_count))
    print('Total number of SAN_NVME_ECT_BASELINE docs : ', sum(san_nvme_ect_doc_count))
    print('Total number of SAN_NVME_ROLLUP docs : ', sum(san_nvme_rollup_doc_count))


    total_docs = pmdb_sanportratedata_doc_count + pmdb_slowdraincounterdata_doc_count + \
                sum(fc_flows_doc_count) + sum(san_ect_doc_count) + sum(san_fc_flows_rollup_doc_count) + \
                sum(fc_nvme_flows_doc_count) + sum(san_nvme_ect_doc_count) + sum(san_nvme_rollup_doc_count)
                
    print('\nTotal number of docs to be fetched = ', total_docs)
    est_time = (total_docs * EST_TIME_PER_DOC)
    print('Approximate time to fetch the required data : {} seconds (~{} minutes)'.format(est_time//(min(14, MAX_PROCS)), est_time//(min(14, MAX_PROCS)*60)) )
    if not MP_USED:
        print('''
        NOTE : This time is based on the assumption that only 1 core is being used. 
        Please use --cores arg to increase the number of cores, which will result in lesser execution time.
        ''')
    elif MP_USED and est_time//60 >= 60 and MAX_PROCS < 32:
        print('Consider using more cores to decrease the total fetch time.')
        for i in range(2, 15, 2):
            if i>MAX_PROCS:
                print('Using {} cores, estimated time can be lowered to {} minutes'.format(i, est_time//(i*60)))


    while True:
        user_input = input('Do you want to continue? Y/N : ')
        if user_input == 'N' or user_input == 'n':
            print('Exiting...')
            sys.exit()
        elif user_input == 'Y' or user_input == 'y':
            while True:
                user_input = input('Do you want to modify the number of cores? Y/N : ')
                if user_input == 'Y' or user_input == 'y':
                    MAX_PROCS = int(input('Enter the number of cores to be used : '))
                    if MAX_PROCS == 0:
                        print('Cannot set number of cores to 0 !!! Setting it to 1.')
                        MAX_PROCS = 1
                    elif MAX_PROCS > 32:
                        print('Cannot set cores to {}. Setting it to maximum cores allowed (ie. 32).'.format(MAX_PROCS))
                        MAX_PROCS = 32
                    break
                elif user_input == 'N' or user_input == 'n':
                    print('Continuing with the user specified cores : {}'.format(MAX_PROCS))
                    break
                else:
                    print('Invalid input. Please try again !')
            print('Running the script with {} core(s) ...'.format(MAX_PROCS))
            break
        else:
            print('Invalid input, please try again !')

    print('Approximate time to fetch the required data : {} seconds (~{} minutes)'.format(est_time//(min(14, MAX_PROCS)), est_time//(min(14, MAX_PROCS)*60)) )
    
    if pmdb_sanportratedata or pmdb_slowdraincounterdata:
        print('\nPerformance Monitoring enabled. Fetching pmdb data as well.')

    # Fetching data from pmdb_slowdraincounterdata.
    if index_to_fetch['slowdraincounter']:
        if pmdb_slowdraincounterdata:
            start = time.time()
            index = 'pmdb_slowdraincounterdata'
            filename = index + '_fetched_data.csv'
            print('Collecting data for index : ', index)
            data = []
            resp = helpers.scan(es, index = index, query = match_all, raise_on_error = False)
            for doc in resp:
                doc = doc['_source']
                doc['swName'] = mapping_dict[doc['swName']]
                data.append(doc)
            end = time.time()
            if len(data):
                write_to_csv_file(filename, data)
                if VERBOSE :
                    print('\nSLOWDRAINCOUNTER DATA : Total docs = {} Time Taken = {} seconds \n', len(data), end-start)
                    print('Data saved to file : ', filename)

                if REMOTE:
                    with open(filename, 'rb') as f_in:
                        with gzip.open(filename + '.gz', 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    copy_cmd = 'sshpass -p {} scp -o StrictHostKeyChecking=no {} {}@{}:{}'.format(PASSWORD, filename + '.gz', USERNAME, REMOTE_IP, REMOTE_PATH)
                    os.system(copy_cmd)
                    logging.warning('Copying the generated csv file to remote location ...')
                    logging.warning('Deleting file {} from local workspace ...'.format(filename))
                    os.remove(filename)
                    os.remove(filename + '.gz')
                    if VERBOSE :
                        print('\n{} successfully copied to remote location and deleted from local space.'.format(filename))
                else:
                    generated_csv_filelist.append(filename)
    else:
        print('Skipping fetching data for index : pmdb_slowdraincounterdata')

    # Fetching data from pmdb_sanportratedata. Can be a large index, hence following all safety checks.
    if index_to_fetch['sanportrate']:
        if pmdb_sanportratedata:
            index = 'pmdb_sanportratedata'
            filename = index + '_fetched_data.csv'
            print('Collecting data for index : ', index)
            data = []
            sub_total_doc = 0
            resp = helpers.scan(es, index = index, query = match_all, raise_on_error = False)
            try : 
                for raw_doc in resp:
                    doc = {x: raw_doc['_source'][x] for x in pmdb_sanportratedata_concerned_keys}
                    doc['switchName'] = mapping_dict[doc['switchName']]
                    data.append(doc)

                    # Keep writing chuks of 100000 docs to csv and free runtime memory so the process does not killed due to OoM Error.
                    if len(data) == 100000:
                        # This appends the data to the existing csv file
                        # logging.warning('Appending a chunk of %s docs to %s', len(data), filename)
                        show_dot_progress(data, 100000)
                        write_to_csv_file(filename, data)
                        sub_total_doc += len(data)
                        data.clear()
                        continue
            except Exception as e:
                logging.error('Got an exception with error: %s', e)
            # Writing the last chunk of data to csv file.
            if len(data):
                # This appends the data to the existing csv file
                # logging.warning('Appending a chunk of %s docs to %s', len(data), filename)
                write_to_csv_file(filename, data)
                sub_total_doc += len(data)
                data.clear()

            if sub_total_doc:
                if VERBOSE :
                    print('\nSANPORTRATE DATA : Total docs = {} Time Taken = {} seconds \n', sub_total_doc, end-start)
                    print('Data saved to file : ', filename)

                if REMOTE:
                    with open(filename, 'rb') as f_in:
                        with gzip.open(filename + '.gz', 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    copy_cmd = 'sshpass -p {} scp -o StrictHostKeyChecking=no {} {}@{}:{}'.format(PASSWORD, filename + '.gz', USERNAME, REMOTE_IP, REMOTE_PATH)
                    os.system(copy_cmd)
                    logging.warning('Copying the generated csv file to remote location ...')
                    logging.warning('Deleting file {} from local workspace ...'.format(filename))
                    os.remove(filename)
                    os.remove(filename + '.gz')
                    if VERBOSE :
                        print('\n{} successfully copied to remote location and deleted from local space.'.format(filename))
                else:
                    generated_csv_filelist.append(filename)
    else:
        print('Skipping fetching data for index : pmdb_sanportratedata')

    # Getting fc_flows data
    if index_to_fetch['scsi'] and index_to_fetch['rawdata']:
        base_index = 'fc_flows_' + switchname.lower()
        if VERBOSE :
            print('Searching for all indices starting with ', base_index)
        
        start = time.time()
        processes = []
        return_list = manager.list()
        file_list = manager.list()

        for index in es.indices.get('*'):
            if base_index in index:
                p = multiprocessing.Process(target=multiprocessing_func, args=(index, match_itl_for_fc_flows, file_list, return_list, mapping_dict, VERBOSE, 'fc_flows', SWIP, REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH))
                processes.append(p)
                print('\nCollecting data for index : ', index)
                p.start()
                time.sleep(5)
                if len(processes) == MAX_PROCS:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        generated_csv_filelist += file_list
        print('\nFC FLOWS : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for fc_flows indices')

    # Getting san_ect_baseline data
    if index_to_fetch['scsi'] and index_to_fetch['enrichdata']:
        base_index = 'san_ect_baseline_' + switchname.lower()
        if VERBOSE :
            print('Searching for all indices starting with ', base_index)
        
        start = time.time()
        processes = []
        return_list = manager.list()
        file_list = manager.list()

        for index in es.indices.get('*'):
            if base_index in index:
                p = multiprocessing.Process(target=multiprocessing_func, args=(index, match_itl_for_san_ect_baseline, file_list, return_list, mapping_dict, VERBOSE, 'san_ect_baseline', SWIP, REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH))
                processes.append(p)
                print('\nCollecting data for index : ', index)
                p.start()
                time.sleep(5)
                if len(processes) == MAX_PROCS:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        generated_csv_filelist += file_list
        print('\nSAN ECT BASELINE : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for san_ect_baseline indices')

    # Getting san_fc_flows_rollup data
    if index_to_fetch['scsi'] and index_to_fetch['rollupdata']:
        base_index = 'san_fc_flows_rollup_'
        if VERBOSE :
            print('Searching for all indices starting with ', base_index)
        
        start = time.time()
        processes = []
        return_list = manager.list()
        file_list = manager.list()

        for index in es.indices.get('*'):
            if base_index in index:
                p = multiprocessing.Process(target=multiprocessing_func, args=(index, match_itl_for_rollups, file_list, return_list, mapping_dict, VERBOSE, 'san_fc_flows_rollup', SWIP, REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH))
                processes.append(p)
                print('\nCollecting data for index : ', index)
                p.start()
                time.sleep(1)
                if len(processes) == MAX_PROCS:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        generated_csv_filelist += file_list
        print('\nSAN FC FLOWS ROLLUP : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for san_fc_flows_rollup indices')
    
    # NOTE : Removing 'lun' field from the concerned keys as san_nvme_rollup and san_nvme_ect_baseline do not support 'lun' field.
    concerned_keys.remove('lun')
    concerned_keys.extend(['ci', 'ni'])

    # Getting fc_nvme_flows data
    if index_to_fetch['nvme'] and index_to_fetch['rawdata']:
        base_index = 'fc_nvme_flows_' + switchname.lower()
        if VERBOSE :
            print('Searching for all indices starting with ', base_index)
        
        start = time.time()
        processes = []
        return_list = manager.list()
        file_list = manager.list()

        for index in es.indices.get('*'):
            if base_index in index:
                p = multiprocessing.Process(target=multiprocessing_func, args=(index, match_itl_for_fc_flows, file_list, return_list, mapping_dict, VERBOSE, 'fc_nvme_flows', SWIP, REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH))
                processes.append(p)
                print('\nCollecting data for index : ', index)
                p.start()
                time.sleep(5)
                if len(processes) == MAX_PROCS:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        generated_csv_filelist += file_list
        print('\nFC NVME FLOWS : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for fc_nvme_flows indices')

    # Getting san_nvme_ect_baseline data
    if index_to_fetch['nvme'] and index_to_fetch['enrichdata']:
        base_index = 'san_nvme_ect_baseline_' + switchname.lower()
        if VERBOSE :
            print('Searching for all indices starting with ', base_index)
        
        start = time.time()
        processes = []
        return_list = manager.list()
        file_list = manager.list()

        for index in es.indices.get('*'):
            if base_index in index:
                p = multiprocessing.Process(target=multiprocessing_func, args=(index, match_itl_for_san_ect_baseline, file_list, return_list, mapping_dict, VERBOSE, 'san_nvme_ect_baseline', SWIP, REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH))
                processes.append(p)
                print('\nCollecting data for index : ', index)
                p.start()
                time.sleep(5)
                if len(processes) == MAX_PROCS:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        generated_csv_filelist += file_list
        print('\nSAN NVME ECT BASELINE : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for san_nvme_ect_baseline indices')

    # Getting san_nvme_rollup data
    if index_to_fetch['nvme'] and index_to_fetch['rollupdata']:
        base_index = 'san_nvme_rollup_'
        if VERBOSE :
            print('Searching for all indices starting with ', base_index)
        
        start = time.time()
        processes = []
        return_list = manager.list()
        file_list = manager.list()

        for index in es.indices.get('*'):
            if base_index in index:
                p = multiprocessing.Process(target=multiprocessing_func, args=(index, match_itl_for_rollups, file_list, return_list, mapping_dict, VERBOSE, 'san_nvme_rollup', SWIP, REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH))
                processes.append(p)
                print('\nCollecting data for index : ', index)
                p.start()
                time.sleep(1)
                if len(processes) == MAX_PROCS:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")  
        end = time.time()
        generated_csv_filelist += file_list
        print('\nSAN NVME ROLLUP : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for san_nvme_rollup indices')

    if not REMOTE:
        print('Compressing all generated csv files, please wait ...')
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        zipfile_name = 'SAN_Insights-' + timestamp
        generate_zip_object(generated_csv_filelist, zipfile_name)
    else:
        print('All the data has been successfully generated at the specified remote location.')

    print('Logfile generated : ', logfile_name)

    #Cleanup
    if os.path.exists('test.txt'):
        os.remove('test.txt')
    if os.path.exists('test_out.txt'):
        os.remove('test_out.txt')
    if os.path.exists('tmp.txt'):
        os.remove('tmp.txt')

    if VERBOSE:
        print('Switch names are mapped to abstract names in the csv files generated.')
        logging.warning('Switch names are mapped to abstract names in the csv files generated.')
        for key, value in mapping_dict.items():
            logging.warning('%s   -->   %s', key, value)
            print(key, ' '*(30-len(key)), '-->', ' '*10 , value)
    
if __name__ == "__main__":
    main(sys.argv[1:])