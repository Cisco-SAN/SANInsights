SAN Insights (SI) Data Fetch Script
————————————————
This script can be used to take a snapshot of the SI database contents for analysis by Cisco. The data is captured in a .csv format in plain text format and does not include any PII like IP addresses or switch/device-alias names. The generated .csv files only includes the ITL (Initiator/Target FCIDs and LUNID) information and the metrics for each ITL over time. It may also capture switch interface metrics if Performance Monitoring was enabled on DCNM.

Cisco will use this data for statistical analysis of the metrics in an anonymised manner.

This script has to be run on the server where DCNM-SI is installed by specifying the IP address of the SAN Analytics enabled MDS switch for which ITL metric data needs to be extracted. The data extraction process can be time consuming depending on how many ITLs were seen on the MDS switch. The script also prints the total run time estimate as part of an interactive dialog. 

Various filters can be used to extract metrics for a subset of the ITLs if desired. When using filters it is recommended to include a representative set of ITLs corresponding to different types of workloads present in the environment. This way the I/O behaviours of different types of workloads can be captured. For example: Specify an “I” filter to include a Database Server; Specify a “T” filter of a flash storage array port.

The script also allows an option to dedicate more CPU cores for the script to speed up the execution time.

Once the script has run successfully, the csv files generated will be zipped into a SAN_Insights-<Timestamp>.zip file. However, if the —remote option is specified, the file would be SCP-ed into the specified remote location.
The generated zip file can be unzipped and the constituent .csv files can be manually examined to make sure no PII data was captured.

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

--swip : Mandatory argument to be provided to fetch the data for a particular switch.
Example: python3 si_fetch_data.py --swip 10.10.10.1

--exclude : Comma separated keywords for the type of data to be excluded from being fetched. By default, only rawdata is excluded.
Example: python3 si_fetch_data.py --swip 10.10.10.1 --exclude sanportrate,slowdraincounter

--include : Comma separated keywords for the type of data to be included from being fetched. By default, everything is included except rawdata.
Example: python3 si_fetch_data.py --swip 10.10.10.1 --include rawdata

--vsan : Comma separated vsans to be passed as a filter to the DB query. If not specified, all vsans are captured
Example: python3 si_fetch_data.py --swip 10.10.10.1 --vsan 1
		python3 si_fetch_data.py --swip 10.10.10.1 --vsan 1,2

--port : Comma separated FC ports to be passed as a filter to the DB query. If not specified, all ports are captured
Example: python3 si_fetch_data.py --swip 10.10.10.1 --port fc1/1
		python3 si_fetch_data.py --swip 10.10.10.1 --port fc1/1,fc1/2

--init : Comma separated Initiator FCID(s) to be passed as a filter to the DB query. If not specified, all the initiator information is captured
Example: python3 si_fetch_data.py --swip 10.10.10.1 --init 0x111111
		 python3 si_fetch_data.py --swip 10.10.10.1 --init 0x111111,0x222222

--target : Comma separated Target FCID(s) to be passed as a filter to the DB query. If not specified, all the target information is captured
Example: python3 si_fetch_data.py --swip 10.10.10.1 --target 0x333333
		 python3 si_fetch_data.py --swip 10.10.10.1 --target 0x333333,0x444444

--lun : Comma separated SCSI LUN(s) to be passed as a filter to the DB query. If not specified, all LUN information is captured
Example: python3 si_fetch_data.py --swip 10.10.10.1 --lun 0000-0000-0000-0002, 0000-0000-0000-0004

--nsid : Comma separated NVMe NSID(s) to be passed as a filter to the DB query. If not specified, all NSID information is captured
Example: python3 si_fetch_data.py --swip 10.10.10.1 --nsid 100,101

A combination of all the above options can be provided for a combination filter filters.
Example: python3 si_fetch_data.py --swip 10.10.10.1 --vsan 1 --init 0x111111 --target 0x333333 --lun 0000-0000-0000-0000
	  python3 si_fetch_data.py --swip 10.10.10.1 --vsan 1 --init 0x111111 --target 0x333333 --nsid 1

--cores : Specify the CPU cores to perform parallel reads to complete the data fetching process faster.
	   Maximum cores supported are 32. Defaults to 1, if not specified.

--remote : Specify the remote path info for scp. Copies data extracted to the remote path provided.
	   Use this option only if there is not enough free disk space on the DCNM server.
	   Specifying this option can increase the total script execution time.

--verbose: Enabling this will print some additional logs on the console.
