# pylint: disable=line-too-long
# pylint: disable=too-many-arguments
# pylint: disable=trailing-whitespace
# pylint: disable=too-many-branches
# pylint: disable=too-many-statements
# pylint: disable=broad-except
# pylint: disable=superfluous-parens
# pylint: disable=missing-module-docstring
# pylint: disable=missing-function-docstring
# pylint: disable=multiple-imports
# pylint: disable=wrong-import-order
# pylint: disable=too-many-locals
# pylint: disable=global-statement

import os
import sys
import json
import signal
import time
import glob
import getopt
import shutil
import logging
import warnings
import csv
from subprocess import STDOUT, PIPE, check_call, run
from collections import defaultdict
import gzip
import multiprocessing
import http.client
from datetime import datetime

def is_root_user():
    cmd = 'id -u'
    proc = run(cmd.split(), stdout=PIPE, stderr=STDOUT)
    output = proc.stdout.decode('utf-8').strip()
    if output == '0':
        return True
    return False

def install_python_package(package):
    print("Installing python package : {}".format(package))
    check_call([sys.executable, "-m", "pip", "install", package])

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    print("Python package 'elasticsearch' is missing!")
    try:
        if is_root_user():
            print("Running as root user, trying to install python package ...")
        else:
            print("Trying to install python package without root permissions ...")
        install_python_package('elasticsearch')
    except Exception as err:
        print('Exception while installing python package, err = ', err)
        sys.exit()
finally:
    try:
        from elasticsearch import Elasticsearch, helpers
    except ImportError:
        print("Python package 'elasticsearch' could not be installed internally!")
        print('''
        Try one of the below ways : 
        1. Run the script as a root user.
        2. Install elasticsearch using => "sudo pip install elasticsearch". If installation is successful, re-run the script.
        ''')
        sys.exit()



warnings.filterwarnings('ignore')

http.client._MAXLINE = 1000000000 # https://stackoverflow.com/questions/63157046/python-http-request-exception-linetoolong

ES_CLEINT = None
DCNM_REMOTE_IP = None
LOCAL_PORT = None
VERBOSE = None
INDEX_TO_FETCH = None
MAPPING_DICT = None


SWIP = None
PORT = None
VSAN = None
INIT = None
TARGET = None
LUN = None
NSID = None

REMOTE = False
# PASSWORD = None
# USERNAME = None 
# REMOTE_IP = None
# REMOTE_PATH = None

EST_FILE_SIZE_PER_DOC = 300 #bytes

LOGFILE_NAME = 'si_fetch_data.log'
logging.basicConfig(filename = LOGFILE_NAME, filemode='w', format='%(asctime)s - %(message)s', level=logging.WARNING)

CONCERNED_KEYS = ['sid', 'did', 'lun', 'port', 'swName', 'timestamp',
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

def get_max_cpu_available():
    cmd = 'nproc'
    proc = run(cmd.split(), stdout=PIPE, stderr=STDOUT)
    output = proc.stdout.decode('utf-8')
    return int(output)

def get_dcnm_version(ip):
    cmd = 'curl -s -k https://{}/fm/fmrest/about/version'.format(ip)
    proc = run(cmd.split(), stdout=PIPE, stderr=STDOUT)
    output = json.loads(proc.stdout)
    ver = output['version']
    return ver

def is_port_free(port):
    if port == None:
        return True
    cmd = 'lsof -i :{}'.format(port)
    proc = run(cmd.split(), stdout=PIPE, stderr=STDOUT)
    output = proc.stdout.decode('utf-8')
    return not bool(output)     # return True if output is empty

def start_port_forwarding(remote_ip):
    print("\n*******************************************\n")
    user = input("Enter USER for DCNM remote ip {} [Recommended : root/admin for RHEL, sysadmin for OVA] : ".format(remote_ip))
    print("Trying to expose remote server {}:9200 to local machine".format(remote_ip))
    print("Searching for a free port on local machine starting from 33500 ...".format(remote_ip))
    global LOCAL_PORT

    for local_port in range(33500, 33600):
        if is_port_free(local_port):
            print("Free port found : {}".format(local_port))
            cmd = 'ssh -N -f -L localhost:{}:localhost:9200 {}@{}'.format(local_port, user, remote_ip)
        else:
            print("Port {} is occupied !".format(local_port))
            continue
        
        print("Running Port Forwarding using SSH using cli : ", cmd)
        run(cmd.split())
        LOCAL_PORT = local_port
        break
    
def pretty_print(header, data):
    for item in header:
        print(item, end = ' '*(25-len(item)))
    print()
    for item in data:
        for field in item:
            print(field, end = ' '*(25-len(str(field))))
        print()

def get_index_count(index_type, switchname):
    count = 0
    string_to_check = None
    if index_type in ['fc_flows', 'san_ect_baseline', 'fc_nvme_flows', 'san_nvme_ect_baseline']:
        string_to_check = index_type + '_' + switchname
    elif index_type in ['sanportrate', 'slowdraincounter']:
        string_to_check = 'pmdb_' + index_type
    else:
        string_to_check = index_type

    for index in ES_CLEINT.indices.get('*'):
        if string_to_check in index:
            count += 1
    return count

def mproc_estimate(local_port, index, switchname, query, return_list):
    establish_es_client(local_port)
    if index in  ['fc_flows', 'san_ect_baseline', 'fc_nvme_flows', 'san_nvme_ect_baseline']:
        index = index + '_' + switchname
    elif index in ['sanportrate', 'slowdrain']:
        index = 'pmdb_' + index

    index += '*'
    resp = helpers.scan(ES_CLEINT, index = index, query = query)
    data = []
    for doc in resp:
        data.append(doc['_source'])
        if len(data) == 5000:
            break
    return_list.append(len(data))

def get_estimated_time_for_download(switchname, index_doc_count_list, cores):
    print('\nGetting the estimated time to run the job based on the internet speed ...')

    requires_estimation = {'sanportrate' : INDEX_TO_FETCH['sanportrate'],
                            'slowdraincounter' : INDEX_TO_FETCH['slowdraincounter'],
                            'fc_flows' : INDEX_TO_FETCH['scsi'] and INDEX_TO_FETCH['rawdata'],
                            'fc_nvme_flows' : INDEX_TO_FETCH['nvme'] and INDEX_TO_FETCH['rawdata'],
                            'san_ect_baseline' : INDEX_TO_FETCH['scsi'] and INDEX_TO_FETCH['enrichdata'],
                            'san_nvme_ect_baseline' : INDEX_TO_FETCH['nvme'] and INDEX_TO_FETCH['enrichdata'],
                            'san_fc_flows_rollup' : INDEX_TO_FETCH['scsi'] and INDEX_TO_FETCH['rollupdata'],
                            'san_nvme_rollup' : INDEX_TO_FETCH['nvme'] and INDEX_TO_FETCH['rollupdata']
                        }

    index_type_list = ['sanportrate', 'slowdraincounter', 'fc_flows', 'san_ect_baseline', 'san_fc_flows_rollup', 'fc_nvme_flows', 'san_nvme_ect_baseline', 'san_nvme_rollup']
    est_time = 0    #in minutes
    report = []
    for index_type, doc_cnt in zip(index_type_list, index_doc_count_list):
        if doc_cnt and requires_estimation[index_type]:
            start_time = time.time()
            manager = multiprocessing.Manager()
            processes = []
            return_list = manager.list()
            index_count = get_index_count(index_type, switchname)
            divisor = min(index_count, cores)
            # print("index_type = {} index_count = {} cores = {}, divisor = {}".format(index_type, index_count, cores, divisor))
            for _ in range(divisor):
                proc = multiprocessing.Process(target=mproc_estimate, args=(LOCAL_PORT, index_type, switchname, match_all, return_list))
                processes.append(proc)
                proc.start()

            for process in processes:
                process.join()

            total_docs_read = sum(return_list)
            end_time = time.time()

            download_rate = (end_time-start_time)/total_docs_read
            report.append( (index_type, int(1/download_rate), doc_cnt, int((doc_cnt * download_rate)//60)) )
            # print('Index type : {} Download Rate : {} docs/sec Doc count : {} Estimated_time : {} minutes'.format(index_type, int(1/download_rate), doc_cnt, (doc_cnt * download_rate)//60 ))
            est_time += (doc_cnt * download_rate)

    header = ['Index Type', 'Download Rate(docs/sec)', 'Doc Count', 'Estimated Time(in minutes)']
    pretty_print(header, report)
    return est_time # return time in minutes

def establish_es_client(local_port):
    global ES_CLEINT

    try:
        localhost = '127.0.0.1'
        ES_CLEINT = Elasticsearch(hosts=[localhost], port=local_port, verify_certs=False, use_ssl=True, retry_on_timeout=True, max_retries=20, timeout=100) # for Linux
        if ES_CLEINT.ping():
            # print("Connected to {}:{}".format(localhost, local_port))
            pass
        else:
            ES_CLEINT = Elasticsearch(hosts=[localhost], port=local_port, verify_certs=False, use_ssl=False, retry_on_timeout=True, max_retries=20, timeout=100) # for OVA/SE
            if ES_CLEINT.ping():
                # print("Connected to {}:{}".format(localhost, local_port))
                pass
            else:
                print("Could not connect to ES {}:{}!".format(localhost, local_port))
                exit_gracefully()

    except Exception as ex:
        print('Connection to Elastcisearch python client failed due to err : ', ex)

def stop_port_forwarding(port):
    print('Stopping port forwarding on port : {}'.format(port))
    cmd = 'lsof -i :{}'.format(port)
    proc = run(cmd.split(), stdout = PIPE, stderr = PIPE)
    pid = proc.stdout.decode('utf-8').strip().split('\n')[-1].split()[1]
    print('Process PID responsible for port forwarding : {}'.format(pid))
    try:
        os.system('kill -9 {}'.format(pid))
    except Exception:
        print('Could not kill PID {}'.format(pid))
    else:
        print('PID {} killed successfully'.format(pid))

def exit_gracefully():
    if not is_port_free(LOCAL_PORT):
        stop_port_forwarding(LOCAL_PORT)

    print('Exiting !')
    sys.exit()

def check_and_delete_residual_files():
    print('Checking for residual files from previous run, if any ...')
    
    extensions = ['csv', 'csv.gz']
    residual_files = [] 
    for extension in extensions:
        csv_files = glob.glob('*.{}'.format(extension))
        file_types = ['rawdata', 'enrichdata', 'rollup']

        for filename in csv_files:
            if any(file_type in filename for file_type in file_types):
                residual_files.append(filename)

    if residual_files:
        print("Residual files found : ", len(residual_files))
        for filename in residual_files:
            print(filename)

        user_input = input('The above file(s) need to be deleted! Do you want to continue? Y/N : ')
        if user_input.lower() == 'n':
            print('Please review and retain the files you need.')
            exit_gracefully()
        elif user_input.lower() == 'y':
            for filename in residual_files:
                print('Deleting file : ', filename)
                os.remove(filename)
    else:
        print("No residual files found. Continuing ...")

def signal_handler(sig, frame):
    print('\nYou pressed Ctrl+C!')
    exit_gracefully()

def hex2dec(hex_string):
    hex_string = hex_string.partition('x')[2]
    base = 16
    
    return int(hex_string, base)
    
def get_ip_to_switchname_mapping():
    switch_name_dict = dict()
    resp = helpers.scan(ES_CLEINT, index = 'dcmdb-switch', query = match_all)
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

    for index in ES_CLEINT.indices.get('*'):
        if 'san_fc_flows_rollup_' in index:
            index_to_be_checked = index
            break
    resp = helpers.scan(ES_CLEINT, index = index_to_be_checked, query = null_check_query)
    count = 0
    for _ in resp:
        count += 1
    
    #print('\nVerifying index {} for null values for VSAN field. Null values found = {}'.format(index_to_be_checked, count))
    if count > 0:
        # print('WARNING : VSAN field missing in rollup indices. Ignoring VSAN filter for rollup indices.')
        return False

    # print('Sanity check passed.')
    return True

def get_switchname_from_swip(ip):
    # print('\nSearching for the switchname for the given switch IP ...')
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

    resp = helpers.scan(ES_CLEINT, index='dcmdb-switch', query=swip_switchname_query)
    query_data = [doc['_source'] for doc in resp]
    count = len(query_data)
    #print('Doc count : ', count)
    
    if count == 0:
        print('Switch IP not found, please try again with different input.')
        logging.error('Switch IP not found, please try again with different input.')
        exit_gracefully()
    elif count == 1:
        switchname = query_data[0]['sysName']
        # print('Switch found : ', switchname)
        logging.warning('Switch found : %s', switchname)
        return switchname
    else:
        print('WARNING : Multiple switches with same IP found !')
        logging.warning('Multiple switches with same IP found !')
        for data in query_data:
            logging.warning(data['sysName'])
        logging.warning('Picking the first switch ...')    
        switchname = query_data[0]['sysName']
        # print('Switch found : ', switchname)
        return switchname
    
def write_to_csv_file(filename, data):
    total, used, free = shutil.disk_usage('/')
    # logging.warning('Estimated disk space required : %s', EST_FILE_SIZE_PER_DOC * len(data))
    if free < EST_FILE_SIZE_PER_DOC * len(data):
        logging.error('Not enough disk space to write more data.')
        print('Not enough disk space to write more data.')
        exit_gracefully()
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
    if switch:
        print('\nGenerating report for switch {}, please wait ...'.format(switch))
    else:
        print('\nGenerating report, please wait ...')

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
    for index in ES_CLEINT.indices.get('*'):
        if 'san_ect_baseline' in index :
            switchname = index.split('_')[3]
            resp = ES_CLEINT.search(index = index, body = distinct_itl_query)
            report[switchname].append(resp['aggregations']['distinct_itls']['value'])

        if 'san_nvme_ect_baseline' in index :
            switchname = index.split('_')[4]
            resp = ES_CLEINT.search(index = index, body = distinct_itcn_query)
            report[switchname].append(resp['aggregations']['distinct_itcns']['value'])

    report_data = []
    for key, value in report.items():
        report_data.append( (key, max(value)) )

    if switch:
        if switch in report.keys():
            return
        print('No data present for specifed switch.')
        exit_gracefully()
    else:
        header = ['Switchname', 'ITL/ITCN Count']
        pretty_print(header, report_data)

def compress_csv_file(csv_file):
    with open(csv_file, 'rb') as f_in:
        with gzip.open(csv_file + '.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(csv_file)
    logging.warning('Deleting file %s', csv_file)

# def compress_csv_file_list(csv_file_list):
#     print("\nCompressing {} csv files ...".format(len(csv_file_list)))
#     start_time = time.time()
#     processes = []
#     for csv_file in csv_file_list:
#         proc = multiprocessing.Process(target=compress_csv_file, args=(csv_file,))
#         print("Starting compression job for csv_file : ", csv_file)
#         processes.append(proc)
#         proc.start()

#     for process in processes:
#         process.join()

#     processes.clear()
#     end_time = time.time()
#     print("Time taken to compress : {} seconds".format(end_time-start_time))
    
def generate_zip_object(csv_gz_file_list, zipfile_name):
    # Moving all the zipped csv files to new directory.
    os.mkdir(zipfile_name)
    for filename in csv_gz_file_list:
        shutil.move(src = filename, dst = zipfile_name)

    print('{} generated successfully.'.format(zipfile_name))

def multiprocessing_func(port, index, query, return_list, mapping_dict, concerned_keys, verbose, index_type, swip):
    # Fetching data for a specified input

    # Establishing a python ELasticsearc client connection for each process.
    establish_es_client(port)
    # print(concerned_keys)

    tmp = index.split('_')
    if index_type in ['fc_flows']:
        tmp[2] = mapping_dict[get_switchname_from_swip(swip)]
    elif index_type in ['fc_nvme_flows', 'san_ect_baseline']:
        tmp[3] = mapping_dict[get_switchname_from_swip(swip)]
    elif index_type in ['san_nvme_ect_baseline']:
        tmp[4] = mapping_dict[get_switchname_from_swip(swip)]

    tmp = '_'.join(x for x in tmp)
    filename = tmp + '_fetched_data.csv'

    # Renaming filenames for better readability
    if index_type == 'fc_flows':
        filename = filename.replace('fc_flows', 'fc_scsi_rawdata')
    elif index_type == 'fc_nvme_flows':
        filename = filename.replace('fc_nvme_flows', 'fc_nvme_rawdata')
    elif index_type == 'san_ect_baseline':
        filename = filename.replace('san_ect_baseline', 'fc_scsi_enrichdata')
    elif index_type == 'san_nvme_ect_baseline':
        filename = filename.replace('san_nvme_ect_baseline', 'fc_nvme_enrichdata')
    elif index_type == 'san_fc_flows_rollup':
        filename = filename.replace('san_fc_flows_rollup', 'fc_scsi_1hr_rollup') 
    elif index_type == 'san_nvme_rollup':
        filename = filename.replace('san_nvme_rollup', 'fc_nvme_1hr_rollup') 

    resp = helpers.scan(ES_CLEINT, index=index, query=query, raise_on_error=False, clear_scroll=False)
    data = []
    sub_total_doc = 0
    try : 
        for raw_doc in resp:
            if index_type in ['fc_flows', 'fc_nvme_flows']:
                doc = raw_doc['_source']
            else:
                doc = {x: raw_doc['_source'][x] for x in concerned_keys}
            
            # fc_flows indices do not contain swName field. They have switchname stored in swIP field.
            if index_type in ['fc_flows', 'fc_nvme_flows']:
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

    except Exception as ex:
        logging.error('Got an exception with error: %s', ex)

    # Writing the last chunk of data to csv file.
    if data:
        # This appends the data to the existing csv file
        # logging.warning('Appending a chunk of %s docs to %s', len(data), filename)
        write_to_csv_file(filename, data)
        sub_total_doc += len(data)
        data.clear()

    if sub_total_doc:
        if verbose :
            print('\nNumber of relevant docs found in index {} : {}'.format(index, sub_total_doc))
            print('Data saved to file : ', filename)

        # if REMOTE:
        #     with open(filename, 'rb') as f_in:
        #         with gzip.open(filename + '.gz', 'wb') as f_out:
        #             shutil.copyfileobj(f_in, f_out)
        #     copy_cmd = 'sshpass -p {} scp -o StrictHostKeyChecking=no {} {}@{}:{}'.format(PASSWORD, filename + '.gz', USERNAME, REMOTE_IP, REMOTE_PATH)
        #     os.system(copy_cmd)
        #     logging.warning('Copying the generated csv file to remote location ...')
        #     logging.warning('Deleting file {} from local workspace ...'.format(filename))
        #     os.remove(filename)
        #     os.remove(filename + '.gz')
        #     if verbose :
        #         print('\n{} successfully copied to remote location and deleted from local space.'.format(filename))
        # else:
        compress_csv_file(filename)
    
        return_list.append(sub_total_doc)
    else:
        return_list.append(0)
        if verbose :
            print('No relevant docs found in index : {}\n'.format(index))

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

def fetch_pmdb_slowdraincounterdata(port, index_to_fetch):
    # Fetching data from pmdb_slowdraincounterdata.
    establish_es_client(port)
    if index_to_fetch['slowdraincounter']:
        start = time.time()
        index = 'pmdb_slowdraincounterdata'
        filename = index + '_fetched_data.csv'
        print('Collecting data for index : ', index)
        data = []
        resp = helpers.scan(ES_CLEINT, index = index, query = match_all, raise_on_error = False)
        for doc in resp:
            doc = doc['_source']
            doc['swName'] = MAPPING_DICT[doc['swName']]
            data.append(doc)
        end = time.time()
        if data:
            write_to_csv_file(filename, data)
            if VERBOSE :
                print('\nSLOWDRAINCOUNTER DATA : Total docs = {} Time Taken = {} seconds \n'.format(len(data), end-start))
                print('Data saved to file : ', filename)
            compress_csv_file(filename)

def fetch_pmdb_sanportratedata(port, index_to_fetch):
    # Fetching data from pmdb_sanportratedata. Can be a large index, hence following all safety checks.
    establish_es_client(port)
    if index_to_fetch['sanportrate']:
        start = time.time()
        index = 'pmdb_sanportratedata'
        filename = index + '_fetched_data.csv'
        print('Collecting data for index : ', index)
        data = []
        sub_total_doc = 0
        resp = helpers.scan(ES_CLEINT, index = index, query = match_all, raise_on_error = False)
        try : 
            for raw_doc in resp:
                doc = {x: raw_doc['_source'][x] for x in pmdb_sanportratedata_concerned_keys}
                doc['switchName'] = MAPPING_DICT[doc['switchName']]
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
        except Exception as err:
            logging.error('Got an exception with error: %s', err)
        # Writing the last chunk of data to csv file.
        if data:
            # This appends the data to the existing csv file
            # logging.warning('Appending a chunk of %s docs to %s', len(data), filename)
            write_to_csv_file(filename, data)
            sub_total_doc += len(data)
            data.clear()

        end = time.time()
        if sub_total_doc:
            if VERBOSE :
                print('\nSANPORTRATE DATA : Total docs = {} Time Taken = {} seconds \n'.format(sub_total_doc, end-start))
                print('Data saved to file : ', filename)
            compress_csv_file(filename)

def main(argv):
    global DCNM_REMOTE_IP, SWIP, PORT, VSAN, INIT, TARGET, LUN, NSID, VERBOSE
    # global REMOTE, PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH
    global CONCERNED_KEYS, INDEX_TO_FETCH, MAPPING_DICT

    job_start_time = time.time()

    # rawdata is set to False be default.
    INDEX_TO_FETCH = {"scsi" : True, "nvme" : True, "rawdata" : False, "enrichdata" : True, "rollupdata" : True, "sanportrate" : True, "slowdraincounter" : True}

    query_internals_for_fc_flows = []
    query_internals_for_san_ect_baseline = []
    query_internals_for_rollups = []

    max_procs = 4
    manager = multiprocessing.Manager() # Shared resource between different processes
    pmdb_sanportratedata = False
    pmdb_slowdraincounterdata = False

    # Check and delete residual files before writing new files
    check_and_delete_residual_files()
    
    try:
        opts, args = getopt.getopt(argv, "hd:s:e:i:p:v:i:t:l:n:v", ['help', 'dcnm=', 'swip=', 'exclude=', 'include=', 'port=', 'vsan=', 'init=', 'target=', 'lun=', 'nsid=', 'verbose',])
        for opt, arg in opts:
            if opt in ("--help"):
                print ('''
                Usage: python3 si_fetch_data.py 
                --dcnm <DCNM remote ip>
                --swip <switch ip> 
                [--exclude <comma separated keywords : scsi, nvme, enrichdata, rollup, sanportrate, slowdraincounter]
                [--include] <comma separted keywords : rawdata]
                [--vsan <vsan>]
                [--port <port>]
                [--init <comma separated Initiator FCIDs>]
                [--target <comma separated Target FCIDs>]
                [--lun <comma separated LUNs for SCSI>]
                [--nsid <comma separated NSIDs for NVMe>]
                [--cores <number of cores>]          
                [--verbose]
                ''')
                exit_gracefully()
            elif opt in ("--verbose"):
                VERBOSE = True
            elif opt in ("--dcnm"):
                DCNM_REMOTE_IP = arg
                ver = get_dcnm_version(DCNM_REMOTE_IP)
                print("Running DCNM version {}".format(ver))
                if ver.split('.')[0] != '11':
                    print("DCNM version {} not supported ! Exiting ...")
                    sys.exit()
                # Opens a port on local machine from the DCNM running on remote server.
                start_port_forwarding(DCNM_REMOTE_IP) # sets the global variable LOCAL_PORT
                establish_es_client(LOCAL_PORT)
            elif opt in ("--swip"):
                SWIP = arg
                query_internals_for_fc_flows.append({"term": {"swIP": get_switchname_from_swip(SWIP)}})
                query_internals_for_san_ect_baseline.append({ "term": {"swIP": SWIP}})
                query_internals_for_rollups.append({ "term": {"swIP": SWIP}})
            elif opt in ("--exclude"):
                excludes = arg.split(',')
                for item in excludes:
                    if item in INDEX_TO_FETCH.keys():
                        INDEX_TO_FETCH[item] = False
                    else:
                        print('Wrong arg passed to excludes. Please check --help !')
                        exit_gracefully()
            elif opt in ("--include"):
                includes = arg.split(',')
                for item in includes:
                    if item in INDEX_TO_FETCH.keys():
                        INDEX_TO_FETCH[item] = True
                    else:
                        print('Wrong arg passed to includes. Please check --help !')
                        exit_gracefully()
            elif opt in ("--port"):
                PORT = arg.split(',')
                if not port_sanity_check(PORT):
                    exit_gracefully()
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
                LUN = arg.split(',')
                query_internals_for_fc_flows.append({ "terms": {"lun": LUN}})
                query_internals_for_san_ect_baseline.append({ "terms": {"lun": LUN}})
                query_internals_for_rollups.append({ "terms": {"lun": LUN}})
            elif opt in ("--nsid"):
                NSID = arg.split(',')
                query_internals_for_fc_flows.append({ "terms": {"ni": NSID}})
                query_internals_for_san_ect_baseline.append({ "terms": {"ni": NSID}})
                query_internals_for_rollups.append({ "terms": {"ni": NSID}})
            # elif opt in ("--cores"):
            #     max_procs = int(arg)
            #     if max_procs <=0 :
            #         print('Cannot set number of cores to {} !!! Setting it to 1.'.format(max_procs))
            #         max_procs = 1
            # elif opt in ("--remote"):
            #     REMOTE = True

    except getopt.error as err:
        print (str(err))
        exit_gracefully()

    # SANITY CHECKS
    if SWIP is None:
        print('Switch IP is a required argument. Please provide a switch ip for which you want to extract the data.')
        exit_gracefully()

    if DCNM_REMOTE_IP is None:
        print('DCNM_REMOTE_IP is a required argument. Please provide a DCNM remote ip for which you want to extract the data.')
        exit_gracefully()

    signal.signal(signal.SIGINT, signal_handler)
    MAPPING_DICT = get_ip_to_switchname_mapping()

    # if REMOTE:
    #     print('Please enter the required info for copying the data to remote location.')
    #     USERNAME = input('Username : ')
    #     PASSWORD = getpass.getpass(prompt='Password : ')
    #     REMOTE_IP = input('Remote IP : ')
    #     REMOTE_PATH = input('NOTE : Please provide a path to a folder that already exists. \nRemote path : ')
    #     cmd = "echo 'Test file for verifying ssh credentials' > test.txt"
    #     os.system(cmd)
    #     print('Verifying SSH credentials ...')
    #     if os.path.exists('test_out.txt'):
    #         os.remove('test_out.txt')
    #     copy_cmd = 'sshpass -p {} scp -o StrictHostKeyChecking=no test.txt {}@{}:{} >> test_out.txt 2>&1'.format(PASSWORD, USERNAME, REMOTE_IP, REMOTE_PATH)
    #     os.system(copy_cmd)
    #     with open('test_out.txt', 'r') as file:
    #         data = file.read()
    #         if 'Permission denied' in data:
    #             print('Please check your SSH credentials and try again. Exiting ...')
    #             exit_gracefully()
    #         else:
    #             print('SSH credentials verified successfully.')

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
        print('\nPrinting query filters...')
        print('query_internals_for_fc_flows : ', query_internals_for_fc_flows)
        print('query_internals_for_san_ect_baseline : ', query_internals_for_san_ect_baseline)
        print('query_internals_for_rollups : ', query_internals_for_rollups)

    # Generating Report
    generate_report()
    
    #print(SWIP, PORT, VSAN, INIT, TARGET)
    switchname = get_switchname_from_swip(SWIP)
    generate_report(switchname.lower())

    fc_flows_doc_count = []
    fc_nvme_flows_doc_count = []
    san_ect_doc_count = []
    san_fc_flows_rollup_doc_count = []
    san_nvme_ect_doc_count = []
    san_nvme_rollup_doc_count = []
    pmdb_sanportratedata_doc_count = 0
    pmdb_slowdraincounterdata_doc_count = 0

    for index in ES_CLEINT.indices.get('*'):
        if 'fc_flows_' + switchname.lower() in index:
            resp = ES_CLEINT.search(body = get_num_docs_query(query_internals_for_fc_flows), index = index)
            fc_flows_doc_count.append(resp['hits']['total'])
        elif 'san_ect_baseline_' + switchname.lower() in index:
            resp = ES_CLEINT.search(body = get_num_docs_query(query_internals_for_san_ect_baseline), index = index)
            san_ect_doc_count.append(resp['hits']['total'])
        elif 'san_fc_flows_rollup_' in index:
            resp = ES_CLEINT.search(body = get_num_docs_query(query_internals_for_rollups), index = index)
            san_fc_flows_rollup_doc_count.append(resp['hits']['total'])
            
        elif 'fc_nvme_flows_' + switchname.lower() in index:
            resp = ES_CLEINT.search(body = get_num_docs_query(query_internals_for_fc_flows), index = index)
            fc_nvme_flows_doc_count.append(resp['hits']['total'])
        elif 'san_nvme_ect_baseline_' + switchname.lower() in index:
            resp = ES_CLEINT.search(body = get_num_docs_query(query_internals_for_san_ect_baseline), index = index)
            san_nvme_ect_doc_count.append(resp['hits']['total'])
        elif 'san_nvme_rollup_' in index:
            resp = ES_CLEINT.search(body = get_num_docs_query(query_internals_for_rollups), index = index)
            san_nvme_rollup_doc_count.append(resp['hits']['total'])
        
        elif index == 'pmdb_sanportratedata':
            pmdb_sanportratedata = True
            resp = ES_CLEINT.search(body = match_all, index = index)
            pmdb_sanportratedata_doc_count = resp['hits']['total']
            
        elif index == 'pmdb_slowdraincounterdata':
            pmdb_slowdraincounterdata = True
            resp = ES_CLEINT.search(body = match_all, index = index)
            pmdb_slowdraincounterdata_doc_count = resp['hits']['total']
    
    # if pmdb_slowdraincounterdata:
    #     print('\nTotal number of SLOWDRAIN_COUNTER_DATA docs : ', pmdb_slowdraincounterdata_doc_count)
    # if pmdb_sanportratedata:
    #     print('\nTotal number of SAN_PORT_RATE_DATA docs : ', pmdb_sanportratedata_doc_count)

    # print('\nTotal number of FC_FLOWS docs : ', sum(fc_flows_doc_count))
    # print('Total number of SAN_ECT_BASELINE docs : ', sum(san_ect_doc_count))
    # print('Total number of SAN_FC_FLOWS_ROLLUP docs : ', sum(san_fc_flows_rollup_doc_count))

    # print('\nTotal number of FC_NVME_FLOWS docs : ', sum(fc_nvme_flows_doc_count))
    # print('Total number of SAN_NVME_ECT_BASELINE docs : ', sum(san_nvme_ect_doc_count))
    # print('Total number of SAN_NVME_ROLLUP docs : ', sum(san_nvme_rollup_doc_count))

    # print("Current cores used : {}      Maximum cores available : {}".format(max_procs, get_max_cpu_available()))
    # if max_procs < get_max_cpu_available():
    #     while True:
    #         user_input = input('Do you want to modify the number of cores? Y/N : ')
    #         if user_input.lower() == 'y':
    #             max_procs = int(input('Enter the number of cores to be used : '))
    #             if max_procs == 0:
    #                 print('Cannot set number of cores to 0! Setting it to 1.')
    #                 max_procs = 1
    #             if max_procs > get_max_cpu_available():
    #                 print("Cannot set cores > max CPU available. Hence, setting cores to {}".format(get_max_cpu_available()))
    #                 max_procs = get_max_cpu_available()
    #             break
    #         if user_input.lower() == 'n':
    #             break
    #         print('Invalid input, please try again !')
    # elif max_procs > get_max_cpu_available():
    #     print("Cannot set cores > max CPU available. Hence, setting cores to {}".format(get_max_cpu_available()))
    #     max_procs = get_max_cpu_available()

    print('Running script with {} core(s) ...'.format(max_procs))

    index_doc_count_list = [pmdb_sanportratedata_doc_count, pmdb_slowdraincounterdata_doc_count, \
                            sum(fc_flows_doc_count), sum(san_ect_doc_count), sum(san_fc_flows_rollup_doc_count), \
                            sum(fc_nvme_flows_doc_count), sum(san_nvme_ect_doc_count), sum(san_nvme_rollup_doc_count)]

    est_time = get_estimated_time_for_download(switchname.lower(), index_doc_count_list, max_procs)
    print('\nApproximate time to fetch the required data : ~ {} minutes'.format(est_time//60))

    user_input = input("Do you want to continue? Y/N : ")
    if user_input.lower() == 'n':
        exit_gracefully()

    if pmdb_slowdraincounterdata or pmdb_sanportratedata:
        print('\nPerformance Monitoring enabled. Fetching pmdb data as well.')

    processes = []

    if pmdb_slowdraincounterdata:
        proc = multiprocessing.Process(target=fetch_pmdb_slowdraincounterdata, args=(LOCAL_PORT, INDEX_TO_FETCH))
        processes.append(proc)
        proc.start()
    if pmdb_sanportratedata:
        proc = multiprocessing.Process(target=fetch_pmdb_sanportratedata, args=(LOCAL_PORT, INDEX_TO_FETCH))
        processes.append(proc)
        proc.start()

    for process in processes:
        process.join()

    processes.clear()

    # Getting fc_flows data
    if INDEX_TO_FETCH['scsi'] and INDEX_TO_FETCH['rawdata']:
        base_index = 'fc_flows_' + switchname.lower()
        if VERBOSE :
            print('\nSearching for all indices starting with : ', base_index)
        
        start = time.time()
        # processes = []
        return_list = manager.list()
        

        establish_es_client(LOCAL_PORT)
        for index in ES_CLEINT.indices.get('*'):
            if base_index in index:
                proc = multiprocessing.Process(target=multiprocessing_func, args=(LOCAL_PORT, index, match_itl_for_fc_flows, return_list, MAPPING_DICT, CONCERNED_KEYS, VERBOSE, 'fc_flows', SWIP))
                processes.append(proc)
                print('Collecting data for index : ', index)
                proc.start()
                # time.sleep(5)
                if len(processes) == max_procs:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        
        print('\nFC FLOWS : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('\nSkipping fetching data for fc_flows indices')

    # Getting san_ect_baseline data
    if INDEX_TO_FETCH['scsi'] and INDEX_TO_FETCH['enrichdata']:
        base_index = 'san_ect_baseline_' + switchname.lower()
        if VERBOSE :
            print('\nSearching for all indices starting with : ', base_index)
        
        start = time.time()
        # processes = []
        return_list = manager.list()
        
        establish_es_client(LOCAL_PORT)
        for index in ES_CLEINT.indices.get('*'):
            if base_index in index:
                proc = multiprocessing.Process(target=multiprocessing_func, args=(LOCAL_PORT, index, match_itl_for_san_ect_baseline, return_list, MAPPING_DICT, CONCERNED_KEYS, VERBOSE, 'san_ect_baseline', SWIP))
                processes.append(proc)
                print('Collecting data for index : ', index)
                proc.start()
                # time.sleep(5)
                if len(processes) == max_procs:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt")
        end = time.time()
        
        print('\nSAN ECT BASELINE : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for san_ect_baseline indices')

    # Getting san_fc_flows_rollup data
    if INDEX_TO_FETCH['scsi'] and INDEX_TO_FETCH['rollupdata']:
        base_index = 'san_fc_flows_rollup_'
        if VERBOSE :
            print('\nSearching for all indices starting with : ', base_index)
        
        start = time.time()
        # processes = []
        return_list = manager.list()

        establish_es_client(LOCAL_PORT)
        for index in ES_CLEINT.indices.get('*'):
            if base_index in index:
                proc = multiprocessing.Process(target=multiprocessing_func, args=(LOCAL_PORT, index, match_itl_for_rollups, return_list, MAPPING_DICT, CONCERNED_KEYS, VERBOSE, 'san_fc_flows_rollup', SWIP))
                processes.append(proc)
                print('Collecting data for index : ', index)
                proc.start()
                # time.sleep(1)
                if len(processes) == max_procs:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        
        print('\nSAN FC FLOWS ROLLUP : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for san_fc_flows_rollup indices')
    
    # NOTE : Removing 'lun' field from the concerned keys as san_nvme_rollup and san_nvme_ect_baseline do not support 'lun' field.
    CONCERNED_KEYS.remove('lun')
    CONCERNED_KEYS.extend(['ci', 'ni'])

    # Getting fc_nvme_flows data
    if INDEX_TO_FETCH['nvme'] and INDEX_TO_FETCH['rawdata']:
        base_index = 'fc_nvme_flows_' + switchname.lower()
        if VERBOSE :
            print('\nSearching for all indices starting with : ', base_index)
        
        start = time.time()
        # processes = []
        return_list = manager.list()

        establish_es_client(LOCAL_PORT)
        for index in ES_CLEINT.indices.get('*'):
            if base_index in index:
                proc = multiprocessing.Process(target=multiprocessing_func, args=(LOCAL_PORT, index, match_itl_for_fc_flows, return_list, MAPPING_DICT, CONCERNED_KEYS, VERBOSE, 'fc_nvme_flows', SWIP))
                processes.append(proc)
                print('Collecting data for index : ', index)
                proc.start()
                # time.sleep(5)
                if len(processes) == max_procs:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        
        print('\nFC NVME FLOWS : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for fc_nvme_flows indices')

    # Getting san_nvme_ect_baseline data
    if INDEX_TO_FETCH['nvme'] and INDEX_TO_FETCH['enrichdata']:
        base_index = 'san_nvme_ect_baseline_' + switchname.lower()
        if VERBOSE :
            print('\nSearching for all indices starting with : ', base_index)
        
        start = time.time()
        # processes = []
        return_list = manager.list()
        
        establish_es_client(LOCAL_PORT)
        for index in ES_CLEINT.indices.get('*'):
            if base_index in index:
                proc = multiprocessing.Process(target=multiprocessing_func, args=(LOCAL_PORT, index, match_itl_for_san_ect_baseline, return_list, MAPPING_DICT, CONCERNED_KEYS, VERBOSE, 'san_nvme_ect_baseline', SWIP))
                processes.append(proc)
                print('Collecting data for index : ', index)
                proc.start()
                # time.sleep(5)
                if len(processes) == max_procs:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")
        end = time.time()
        
        print('\nSAN NVME ECT BASELINE : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for san_nvme_ect_baseline indices')

    # Getting san_nvme_rollup data
    if INDEX_TO_FETCH['nvme'] and INDEX_TO_FETCH['rollupdata']:
        base_index = 'san_nvme_rollup_'
        if VERBOSE :
            print('\nSearching for all indices starting with : ', base_index)
        
        start = time.time()
        # processes = []
        return_list = manager.list()
        
        establish_es_client(LOCAL_PORT)
        for index in ES_CLEINT.indices.get('*'):
            if base_index in index:
                proc = multiprocessing.Process(target=multiprocessing_func, args=(LOCAL_PORT, index, match_itl_for_rollups, return_list, MAPPING_DICT, CONCERNED_KEYS, VERBOSE, 'san_nvme_rollup', SWIP))
                processes.append(proc)
                print('Collecting data for index : ', index)
                proc.start()
                # time.sleep(1)
                if len(processes) == max_procs:
                    for process in processes:
                        process.join()
                    processes.clear()

        # Running the remaining processes
        for process in processes:
            process.join()
        processes.clear()
        os.system(" curl -s -X DELETE 'localhost:9200/_search/scroll/_all?pretty' > tmp.txt ")  
        end = time.time()
        
        print('\nSAN NVME ROLLUP : Total docs = {} Time Taken = {} seconds \n'.format(sum(return_list), end-start))
    else:
        print('Skipping fetching data for san_nvme_rollup indices')

    print('Total time taken to fetch the required data : {} minutes'.format((time.time()-job_start_time)//60))

    if not REMOTE:
        print('Compressing all generated csv files, please wait ...')
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        zipfile_name = 'SAN_Insights-' + timestamp
        # Get all the .csv.gz files
        extension = 'csv.gz'
        csv_gz_files = glob.glob('*.{}'.format(extension))
        generate_zip_object(csv_gz_files, zipfile_name)
    else:
        print('All the data has been successfully generated at the specified remote location.')

    print('Logfile generated : ', LOGFILE_NAME)

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
        for key, value in MAPPING_DICT.items():
            logging.warning('%s   -->   %s', key, value)
            print(key, ' '*(30-len(key)), '-->', ' '*10 , value)

    print('\nTotal time taken to complete the task : {} minutes'.format((time.time()-job_start_time)//60))
    stop_port_forwarding(LOCAL_PORT)
    
if __name__ == "__main__":
    main(sys.argv[1:])
