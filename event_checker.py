"""
Usage: event_checker.py nodes_state_now nodes_state_early

Description:
Check node event such as:
1. Node down
2. CPU high temperature
3. GPU high temperature
4. InfiniBand adaptor high temperature
5. InfiniBand speed not correct
6. Disk volume high usage
7. Non-pbs user job occur in computing nodes

"""
import sys
import os
import time

sys.dont_write_bytecode = True

import src.run_cli as rc
import src.job_collector as jc

def load_state(file_name):
    if not os.path.isfile(file_name):
        print("No such file: %s" % file_name)
        exit(1)
    
    node_state = {}
    with open(file_name) as f:
        lines = f.readlines()
        
    for line in lines:
        state_data = line.split()
        node_name = state_data[0]
        if not node_name.startswith('eureka'):
            continue
        
        if len(state_data) == 14:
            node_state[node_name] = {
                'Job': {state_data[3]:{
                    'User': state_data[1],
                    'Job_name': state_data[2],
                    'Time_used': state_data[4],
                }},
                '%CPU': state_data[5],
                'CPU_Mem': state_data[6],
                'T_CPU': state_data[7],
                '%GPU': state_data[8],
                'GPU_Mem': state_data[9],
                'T_GPU': state_data[10],
                'IB_speed': state_data[11],
                'T_IB': state_data[12],
                'Disk': state_data[13],
             }
        elif len(state_data) == 4:
            node_state[node_name]['Job'][state_data[2]] = {
                'User'     : state_data[0],
                'Job_name' : state_data[1],
                'Time_used': state_data[3],
            }
        elif len(state_data) == 2:
            node_state[node_name] = {'State': 'Down'}
            continue
        else:
            print("Unexpected file format")
            continue
            
        if '--' in node_state[node_name]['Job']:
            node_state[node_name]['State'] = 'Free'
        else:
            node_state[node_name]['State'] = 'Job_exlusive'

    return node_state

def check_node_down(current_state, previous_state):
    new_down = {}
    for node in current_state:
        if not current_state[node]['State']  == 'Down':
            continue
        elif previous_state[node]['State'] == 'Down':
            continue
        else:
            new_down[node] = {'Job': previous_state[node]['Job']}

    return new_down

def check_CPU_temp(node_state, Temp_limit):
    high_temp_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['T_CPU']) > Temp_limit:
            high_temp_nodes[node] = {'T_CPU': node_state[node]['T_CPU'], 'Job': node_state[node]['Job']}

    return high_temp_nodes

def check_GPU_temp(node_state, Temp_limit):
    high_temp_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['T_GPU']) > Temp_limit:
            high_temp_nodes[node] = {'T_GPU': node_state[node]['T_GPU'], 'Job': node_state[node]['Job']}

    return high_temp_nodes
    
def check_IB_temp(node_state, IB_temp_limit):
    high_temp_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['T_IB']) > IB_temp_limit:
            high_temp_nodes[node] = {'T_IB': node_state[node]['T_IB'], 'Job': node_state[node]['Job']}

    return high_temp_nodes

def check_IB_speed(node_state, IB_speed_threshold):
    low_speed_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['IB_speed']) < IB_speed_threshold:
            low_speed_nodes[node] = {'IB_speed':node_state[node]['IB_speed'], 'Job': node_state[node]['Job']}
    
    return low_speed_nodes

def check_disk_usage(node_state, Disk_usage_threshold):
    high_usage_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['Disk'][:-1]) > Disk_usage_threshold:
            high_usage_nodes[node] = {'Disk': node_state[node]['Disk'], 'Job': node_state[node]['Job']}
    
    return high_usage_nodes

def check_non_pbs_job(current_state, all_jobs, all_users):
    # 1. check non pbs user jobs
    large_job_in_00 = {}
    non_pbs_jobs = {}
    for node in current_state:
        # 1.1 Skip if the node is down
        if current_state[node]['State'] == 'Down': continue
        # 1.2 Check large jobs in login node
        if node == 'eureka00':
            for user in all_jobs[node]:
                large_job_in_00[user] = {}
                for job in all_jobs[node][user]:
                    if float(all_jobs[node][user][job]['%CPU']) > 100.\
                    or float(all_jobs[node][user][job]['%MEM']) > 25:
                        large_job_in_00[user][job] = {
                            '%CPU': all_jobs[node][user][job]['%CPU'],
                            '%MEM': all_jobs[node][user][job]['%MEM'],
                            'Command': all_jobs[node][user][job]['Command'],
                        }
                    
                if len(large_job_in_00[user]) == 0:
                    large_job_in_00.pop(user)
            continue
        # 1.3 Check non pbs user job in computing nodes
        non_pbs_jobs[node] = {}
        for job in all_jobs[node]:
            pbs_user = [ current_state[node]['Job'][job]['User'] for job in current_state[node]['Job'] ]
            for user in all_jobs[node]:
                non_pbs_jobs[node][user] = {}
                if user in pbs_user:
                    continue
                if user in all_users:
                    for job in all_jobs[node][user]:
                    # Exclude the nvidia-cuda-server
                        if 'nvidia-cuda-mps-server' in all_jobs[node][user][job]['Command']:
                            continue
                        non_pbs_jobs[node][user][job] = {
                            '%CPU': all_jobs[node][user][job]['%CPU'],
                            '%MEM': all_jobs[node][user][job]['%MEM'],
                            'Command': all_jobs[node][user][job]['Command'],
                        }
                if len(non_pbs_jobs[node][user]) == 0:
                    non_pbs_jobs[node].pop(user)
                non_pbs_jobs[node]['pbs_user'] = pbs_user
        if len(non_pbs_jobs[node]) == 0:
            non_pbs_jobs.pop(node)
            
    return large_job_in_00, non_pbs_jobs

def alert(event, data, send_mail):
    mail_list = "calab-cluster-admin@googlegroups.com"
    mail_content_file = "/tmp/mail_content"
    local_time = time.localtime()
    content = "%s/%s/%s %s:%s\n" %(
        local_time.tm_year, 
        local_time.tm_mon,
        local_time.tm_mday,
        local_time.tm_hour,
        local_time.tm_min,
    )
    with open(mail_content_file,'w') as f:
        if 'temp' in event:
            content += "%-12s %-16s %-16s %-8s %8s\n" %('Node_name', 'User', 'Job_name', 'PID', 'T_' + event.split()[0])
            for node in data:
                counter = 0
                for job in data[node]['Job']:
                    if counter > 0:
                        content += "%-12s %-16s %-16s %-8s %8s\n" %(
                            '', 
                            data[node]['Job'][job]['User'], 
                            data[node]['Job'][job]['Job_name'], 
                            job, 
                            '',
                        )
                    else:
                        content += "%-12s %-16s %-16s %-8s %8s\n" %(
                            node, data[node]['Job'][job]['User'], 
                            data[node]['Job'][job]['Job_name'], 
                            job, 
                            data[node]['T_' + event.split()[0]],
                        )
                    counter += 1

        elif 'speed' in event:
            content += "%-12s %6s\n" %('Node_name', event.split()[0] + '_speed')
            for node in data:
                content += "%-12s %6s\n" %(node, data[node][event.split()[0] + '_speed'])

        elif 'usage' in event:
            content += "%-12s %6s\n" %('Node_name', event.split()[0] + '_usage')
            for node in data:
                content += "%-12s %6s\n" %(node, data[node][event.split()[0]])

        elif 'down' in event:
            content += "%-12s %-16s %-16s %-8s\n" %('Node_name', 'User', 'Job', 'PID')
            for node in data:
                counter = 0
                for job in data[node]['Job']:
                    if counter > 0:
                        content += "%-12s %-16s %-16s %-8s\n" %(
                            '',
                            data[node]['Job'][job]['User'],
                            data[node]['Job'][job]['Job_name'],
                            job,
                        )
                    else:
                        content += "%-12s %-16s %-16s %-8s\n" %(
                            node,
                            data[node]['Job'][job]['User'],
                            data[node]['Job'][job]['Job_name'],
                            job,
                        )
                    counter += 1
        elif "Large job" in event:
            content += "%-16s %-8s %6s %8s %8s %s\n" %('User', 'PID', '%CPU', '%MEM', 'Time', 'Command')
            for user in data:
                for job in data[user]:
                    content += "%-16s %-8s %6s %8s %8s %s\n" %(
                        user, 
                        job, 
                        data[user][job]['%CPU'],
                        data[user][job]['%MEM'],
                        data[user][job]['Time'],
                        data[user][job]['Command'],
                        )
        elif "Non-pbs job" in event:
            content += "%-12s %-16s %-16s %-12s %s\n" %('Node', 'pbs_user', 'non-pbs_user', 'non_pbs_PID', 'Command')
            for node in data:
                pbs_user = data[node].pop('pbs_user')
                for user in data[node]:
                    for job in data[node][user]:
                        content += "%-12s %-16s %-16s %-12s %s\n" %(
                            node,
                            ','.join(pbs_user),
                            user,
                            job,
                            ' '.join(data[node][user][job]['Command']),
                        )
        else:
            print("Unexpected event.")
            exit(1)

        f.write(content)
    
    if send_mail:
        cmd = ['mail', '-s', '"' + event + '"', mail_list, '<' , mail_content_file]
        print(cmd)
        print(content)
        msg = {}
        #rc.run_cli(cmd, msg)

    #os.remove(mail_content_file)
    return 0

if __name__ == '__main__':
    # 0. Load node state files
    current_state = load_state(sys.argv[1])
    previous_state = load_state(sys.argv[2])
    
    # 1. Node down
    new_down_nodes = check_node_down(current_state, previous_state)
    if len(new_down_nodes) > 0:
        alert("Node down", new_down_nodes, True)

    
    # 2. CPU high temperature
    CPU_temp_limit = 65.
    CPU_high_temp_nodes = check_CPU_temp(current_state, CPU_temp_limit)
    if len(CPU_high_temp_nodes) > 0:
        alert("CPU high temp", CPU_high_temp_nodes, True)


    # 3. GPU high temperature
    GPU_temp_limit = 80.
    GPU_high_temp_nodes = check_GPU_temp(current_state, GPU_temp_limit)
    if len(GPU_high_temp_nodes) > 0:
        alert("GPU high temp", GPU_high_temp_nodes, True)


    # 4. IB high temperature
    IB_temp_limit = 100.
    IB_high_temp_nodes = check_IB_temp(current_state, IB_temp_limit)
    if len(IB_high_temp_nodes) > 0:
        alert("IB high temp", IB_high_temp_nodes, True)


    # 5. IB speed incorrect
    IB_speed_threshold = 100.
    IB_low_speed_nodes = check_IB_speed(current_state, IB_speed_threshold)
    if len(IB_low_speed_nodes) > 0:
        alert("IB low speed", IB_low_speed_nodes, True)


    # 6. Disk volume uages
    Disk_usage_threshold = 80
    Disk_high_usage_nodes = check_disk_usage(current_state, Disk_usage_threshold)
    if len(Disk_high_usage_nodes) > 0:
        alert("Disk high usage", Disk_high_usage_nodes, previous_state, True)


    # 7. Non-pbs job 
    alive_nodes = []
    for node in current_state:
        if current_state[node]['State'] != 'Down':
            alive_nodes.append(node)

    all_jobs = jc.get_all_jobs_in_all_nodes(alive_nodes)
    all_users = {}
    rc.run_cli(['ls','/home/'], all_users)
    large_job_in_00, non_pbs_jobs = check_non_pbs_job(current_state, all_jobs, all_users['ls'])

    if len(large_job_in_00) > 0:
        alert("Large job in login node", large_job_in_00, True)

    if len(non_pbs_jobs) > 0:
        alert("Non-pbs job in computing nodes", non_pbs_jobs, True)
