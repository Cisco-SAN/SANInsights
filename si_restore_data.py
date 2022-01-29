from elasticsearch import Elasticsearch, helpers
import multiprocessing
import json, time, csv, os, sys, getopt, shutil, gzip

try:
    es = Elasticsearch(['localhost:9200'], timeout = 10000)
except Exception as e:
    print('Connection to Elastcisearch python client failed due to err : ', e)

MAX_PROCS = 1
VERBOSE = True
DIRECTORY = None

def get_index_mapping_and_setting(type):
    print("Entered into get_index_mapping with type : ", type)
    json_file = type + '.json'
    with open(json_file) as file:
        print('Reading json_file : ', json_file)
        request_body = json.load(file)
        # print(request_body)
    return request_body

def create_index(es, index_name, type):
    if VERBOSE:
        print('Creating index : ', index_name)
    created = False

    try:
        if not es.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es.indices.create(index = index_name, ignore=400, body = get_index_mapping_and_setting(type))
            if VERBOSE:
                print('Created Index : ', index_name)
        else:
            if VERBOSE:
                print('Index already exists.')
        created = True
    except Exception as err:
        if VERBOSE:
            print('Could not create a new index, err = ', err)
           
def multiprocessing_func(directory, file):
    t1 = time.time()
    with open(os.path.join(directory, file)) as f:
        if file.startswith('pmdb_sanportratedata'):
            index_name = 'pmdb_sanportratedata'
            create_index(es, index_name, 'pmdb_sanportratedata')
        elif file.startswith('pmdb_slowdraincounterdata'):
            index_name = 'pmdb_slowdraincounterdata'
            create_index(es, index_name, 'pmdb_slowdraincounterdata')

        elif file.startswith('fc_scsi_rawdata'):
            index_name = file.replace('fc_scsi_rawdata', 'fc_flows').replace('_fetched_data.csv', '')
            create_index(es, index_name, 'fc_flows')
        elif file.startswith('fc_nvme_rawdata'):
            index_name = file.replace('fc_nvme_rawdata', 'fc_nvme_flows').replace('_fetched_data.csv', '')
            create_index(es, index_name, 'fc_nvme_flows')
        
        elif file.startswith('fc_scsi_enrichdata'):
            index_name = file.replace('fc_scsi_enrichdata', 'san_ect_baseline').replace('_fetched_data.csv', '')
            create_index(es, index_name, 'san_ect_baseline')
        elif file.startswith('fc_nvme_enrichdata'):
            index_name = file.replace('fc_nvme_enrichdata', 'san_nvme_ect_baseline').replace('_fetched_data.csv', '')
            create_index(es, index_name, 'san_nvme_ect_baseline')

        elif file.startswith('fc_scsi_1hr_rollup'):
            index_name = file.replace('fc_scsi_1hr_rollup', 'san_fc_flows_rollup').replace('_fetched_data.csv', '')
            create_index(es, index_name, 'san_fc_flows_rollup')
        elif file.startswith('fc_nvme_1hr_rollup'):
            index_name = file.replace('fc_nvme_1hr_rollup', 'san_nvme_rollup').replace('_fetched_data.csv', '')
            create_index(es, index_name, 'san_nvme_rollup')
        
        else:
            print('Should not come here !!!, file = ', file) # .DS_Store
            return

        reader = csv.DictReader(f)
        try:
            for success, info in helpers.parallel_bulk(es, reader, 
                                        index = index_name, 
                                        chunk_size = 1000,
                                        thread_count = 8
                                        ):
                if not success:
                    print('A document failed:', info)
        except helpers.BulkIndexError as exception:
            print('Skipping the errored doc...')
            print(exception)
    t2 = time.time()
    if VERBOSE:
        print('File {} read and pushed to Elastic. Time taken : {} seconds'.format(file, t2-t1))

def main(argv):
    global MAX_PROCS, DIRECTORY
    try:
        opts, args = getopt.getopt(argv, "hc:d:v", ['help','cores=', 'directory=', 'verbose'])
        for opt, arg in opts:
            if opt in ("--help"):
                print ('''
                Usage: python3 restore_csv_to_elastic.py 
                [--cores] <Specify the number of cores to be used. Defaults to 1>
                [--directory] <Specify the directory which contains the data to be restored>
                [--verbose]                 --> Prints logs on the console
                ''')
                sys.exit()
            elif opt in ("--cores"):
                MAX_PROCS = int(arg)
            elif opt in ("--directory"):
                DIRECTORY = arg

    except getopt.error as err:
        print (str(err))
        sys.exit(2)


    start = time.time()
    index_list = []
    if DIRECTORY:
        directory = DIRECTORY
    else:
        directory = input("Please enter the path to data directory : ")
    processes = []
    print('Running the script with {} core(s) ...'.format(MAX_PROCS))

    for file in sorted(os.listdir(directory)):
        with gzip.open(os.path.join(directory, file), 'rb') as f_in:
            with open(os.path.join(directory, file.rstrip('.gz')), 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        os.remove(os.path.join(directory, file))

    for file in sorted(os.listdir(directory)):
        p = multiprocessing.Process(target = multiprocessing_func, args = (directory, file))
        processes.append(p)
        if VERBOSE:
            print('Pushing {} data to Elastic '.format(file))
        p.start()
        if len(processes) == MAX_PROCS:
            for process in processes:
                process.join()
            processes.clear()

    # Running the remaining processes
    for process in processes:
        process.join()
    processes.clear()

    end = time.time()
    print('Total Time taken = {} seconds (~{} minutes)'.format( (end - start), (end - start)//60 ))
                
if __name__ == "__main__":
    main(sys.argv[1:])