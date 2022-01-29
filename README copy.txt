SAN Insights (SI) Data Fetch Script
----------------------------------------------------

DCNM Versions Supported : 11.X on Linux/ISO/OVA

--------- PRE-REQUISITES TO RUN THE SCRIPT ---------
1. Please check if Python 3 is installed
   " which python3 "
   If not installed, please install Python3 based on your Linux version.

2. Check if elasticsearch and tqdm are installed already. If yes, jump directly to " HOW TO RUN THE SCRIPT " step.
   " pip3 list | grep elasticsearch "
   " pip3 list | grep tqdm "
   
3. If any required python package is absent or you face any other issue while running the script, please try installing virtualenv
   " pip3 install virtualenv "

You can encounter some read/write permission issues while installing virtualenv python package.
Possible solutions are:
1. Try using sudo
   sudo pip3 install virtualenv
2. The VM on which script is running may not have access to https://pypi.org/simple
   pip3 install virtualenv --index-url <URL that is accessible from the VM>
   Ex. pip3 install virtualenv --index-url https://aci-docker-reg.cisco.com/artifactory/api/pypi/telemetry-pypi-remote/simple
3. virtualenv --version
   Use the above command to check if virtualenv is installed successfully.

Creating a new python virtual environment:
1. virtualenv si_py
2. Use the new python3 executable path instead of python3 to run the si_fetch_data.py script
   Ex. si_py/bin/python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1

--------- WHAT DOES THIS SCRIPT DO ---------
This script can be used to take a snapshot of the SI database contents for analysis by Cisco. The data is captured in a .csv format in plain text format and does not include any PII like IP addresses or switch/device-alias names. The generated .csv files only includes the metrics for ITL (Initiator/Target FCIDs and LUNID) or ITN (Initiator/Target/NameSpace) for SCSI/NVMe flows over time. It may also capture switch interface metrics if Performance Monitoring was enabled on DCNM.

This script can be run remotely on any Linux based machine by specifying the DCNM IP address and the IP address of the SAN Analytics enabled MDS switch (mgmt0 interface) for which ITL metric data needs to be extracted. The data extraction process can be time consuming depending on how many ITLs were captured on the MDS switch interfaces. The script may attempt to install specific python modules if not already installed. The script also prints the different types of metric data that will be captured along with the total script run-time estimate as part of an interactive dialog. 

Various filters can be used to extract metrics for a subset of the ITLs if desired. When using filters it is recommended to include a representative set of ITLs corresponding to different types of workloads present in the environment. This way the I/O behaviours of different types of workloads can be captured. For example: Specify an “I” filter to include a Database Server; Specify a “T” filter of a flash storage array port.

Once the script has run successfully, the csv files generated will be compressed and moved to a SAN_Insights-<Timestamp> directory. The generated directory can be explored and the constituent .csv.gz files can be manually examined to make sure no PII data was captured. A copy of the .csv.gz files can then be handed over to your Cisco representative for further analysis.

Cisco will use this data for statistical analysis of the metrics in an anonymised manner which would help build algorithms to better solve real world storage performance problems. The analysis can also be shared on request.

--------- HOW TO RUN THE SCRIPT ---------
Usage: python3 si_fetch_data.py
--dcnm <dcnm ip>
--swip <switch_ip> 
[--exclude <comma separated keywords : scsi, nvme, enrichdata, rollup, sanportrate, slowdraincounter]
[--include] <comma separted keywords : rawdata]
[--vsan <vsan>]
[--port <port>]
[--init <comma separated Initiator FCIDs>]
[--target <comma separated Target FCIDs>]
[--lun <comma separated LUNs for SCSI>]
[--nsid <comma separated NSIDs for NVMe>]                 
[--verbose]

--dcnm : Mandatory argument, IP address of the DCNM server
--swip : Mandatory argument, IP address of a SAN Analytics enabled MDS switch
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1

--exclude : Comma separated keywords for the type of data to be excluded from being fetched. By default, only rawdata is excluded.
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --exclude sanportrate,slowdraincounter

--include : Comma separated keywords for the type of data to be included from being fetched. By default, everything is included except rawdata.
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --include rawdata

--vsan : Comma separated vsans to be passed as a filter to the DB query. If not specified, all vsans are captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --vsan 1
	  python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --vsan 1,2

--port : Comma separated FC ports to be passed as a filter to the DB query. If not specified, all ports are captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --port fc1/1
	  python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --port fc1/1,fc1/2

--init : Comma separated Initiator FCID(s) to be passed as a filter to the DB query. If not specified, all the initiator information is captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --init 0x111111
	  python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --init 0x111111,0x222222

--target : Comma separated Target FCID(s) to be passed as a filter to the DB query. If not specified, all the target information is captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --target 0x333333
	  python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --target 0x333333,0x444444

--lun : Comma separated SCSI LUN(s) to be passed as a filter to the DB query. If not specified, all LUN information is captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --lun 0000-0000-0000-0002, 0000-0000-0000-0004

--nsid : Comma separated NVMe NSID(s) to be passed as a filter to the DB query. If not specified, all NSID information is captured
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --nsid 100,101

A combination of all the above options can be provided for a combination filter filters.
Example: python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --vsan 1 --init 0x111111 --target 0x333333 --lun 0000-0000-0000-0000
	  python3 si_fetch_data.py --dcnm 9.9.9.9 --swip 10.10.10.1 --vsan 1 --init 0x111111 --target 0x333333 --nsid 1

--verbose: Enabling this will print some additional logs on the console.

--------- HOW TO INSPECT THE FETCHED DATA ---------
For MacOS/Linux Users:
1. Go to the newly created directory of the form SAN_Insights-*
2. Choose file fo your interest and perform gunzip <filename> in the terminal.
3. The .csv.gz file will be converted to .csv file which can now be opened in Microsoft Excel.
4. Once inspected, please zip it back to .csv.gz format using " gzip <filename> ".

