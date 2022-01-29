import os
from subprocess import STDOUT, PIPE, run, Popen

def check_if_port_is_free(port):
    cmd = 'lsof -i :{}'.format(port)
    proc = run(cmd.split(), stdout=PIPE, stderr=STDOUT)
    output = proc.stdout.decode('utf-8')
    return not bool(output)     # return True if output is empty

def stop_port_forwarding(port):
    print("Stopping port forwarding on port : ", port)
    cmd = 'lsof -i :{}'.format(port)
    print(cmd.split())
    proc = run(cmd.split(), stdout = PIPE, stderr = PIPE )
    pid = proc.stdout.decode('utf-8').strip().split('\n')[-1].split()[1]
    print(pid)
    print("port = {}, pid = {}".format(port, pid))
    print("Stopping port forwarding now. Killing PID : {}".format(pid))
    os.system('kill -9 {}'.format(pid))

if __name__ == "__main__":
    for port in range(33500, 33502):
        if not check_if_port_is_free(port):
            stop_port_forwarding(port)