SAN Insights (SI) Data Fetch Script v2.0
----------------------------------------

*** Only supported for Linux Users ***

DCNM Versions Supported : 11.x (Linux/ISO/OVA)

This script can be used to take a snapshot of the SI database per-switch, per-DCNM instance. The data is captured in a .csv format (plain text) and does not include any PII like IP addresses or switch/device-alias names. The generated .csv files only includes the metrics for ITL (Initiator/Target/LUNID) or ITN (Initiator/Target/NameSpace) for SCSI/NVMe flows over time. It may also capture switch interface metrics if DCNM Performance Monitoring was enabled.

This script can be run remotely on any Linux based machine with reachability to the management network of your fabric. The two mandatory command line arguments for the script are the IP address of DCNM Server and Mgmt IP address of MDS switch. Before the script executes, it may attempt to install specific python modules if not already installed. It also prints the different types of metrics that will be captured and the total script run-time estimate. 

The data extraction process can be time consuming when there are a large number of ITLs for which metrics have to be captured. Various optional filters can also be used to extract metrics for a subset of the ITLs to quicken the script execution time or to save on storage. When using filters due to these requirements, it is recommended to include a representative set of ITLs corresponding to different types of workloads present in the environment. This way the I/O behaviours of different types of workloads can be captured. For example: Specify an “init” filter to include a Database Server; Specify a “target” filter of a flash storage array port.

After the script has run successfully, the .csv files generated will be compressed and moved to a SAN_Insights-<Timestamp> folder. A copy of the .csv.gz files from this folder can then be handed over to your Cisco representative. Cisco will use this data for statistical analysis of the metrics in an anonymised manner which would help build algorithms to better solve real world storage performance problems. The analysis can also be shared on request.

Pre-requisites for running the script:
1) Execute the below command to extract the files
tar -zxf wheelhouse.tar.gz

2) Execute the below command to install the required libs and their dependencies
pip3 install -r wheelhouse/requirements.txt --no-index --find-links wheelhouse


Usage: python3 si_fetch_data.py
--dcnm <dcnm ip>
--swip <switch_ip> 
[--exclude <comma separated keywords : scsi, nvme, enrichdata, rollup, sanportrate, slowdraincounter]
[--include <comma separated keywords : rawdata]
[--vsan <vsan>]
[--port <port>]
[--init <comma separated Initiator FCIDs>]
[--target <comma separated Target FCIDs>]
[--lun <comma separated LUNs for SCSI>]
[--nsid <comma separated NSIDs for NVMe>]                 
[--verbose]

--dcnm : Mandatory argument, IP address of the DCNM server
--swip : Mandatory argument, IP address of a SAN Analytics enabled MDS switch
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1. This is the recommended way to run the script when not constrained by time/storage.

--exclude : Comma separated keywords for the type of data to be excluded from the fetch. By default, only rawdata is excluded.
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --exclude sanportrate,slowdraincounter

--include : Comma separated keywords for the type of data to be included from the fetch. By default, everything is included except rawdata.
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --include rawdata

--vsan : Comma separated vsans to be passed to the fetch. If not specified, all vsans are captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --vsan 1
	 python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --vsan 1,2

--port : Comma separated FC ports to be passed to the fetch. If not specified, all ports are captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --port fc1/1
	 python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --port fc1/1,fc1/2

--init : Comma separated Initiator FCID(s) to be passed to the fetch. If not specified, all the initiator information is captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --init 0x111111
	 python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --init 0x111111,0x222222

--target : Comma separated Target FCID(s) to be passed to the fetch. If not specified, all the target information is captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --target 0x333333
	 python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --target 0x333333,0x444444

--lun : Comma separated SCSI LUN(s) to be passed to the fetch. If not specified, all LUN information is captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --lun 0000-0000-0000-0002,0000-0000-0000-0004

--nsid : Comma separated NVMe NSID(s) to be passed to the fetch. If not specified, all NSID information is captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --nsid 100,101

--verbose: Print additional logs on the console.

A combination of all the above options can be provided for a combination filter filters.
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --vsan 1 --init 0x111111 --target 0x333333 --lun 0000-0000-0000-0000
	  python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --vsan 1 --init 0x111111 --target 0x333333 --nsid 1
