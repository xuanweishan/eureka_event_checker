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
            node_state[node_name] = {'Job'      : {state_data[3]:{
                                                   'User'     : state_data[1],
                                                   'Job_name' : state_data[2],
                                                   'Time_used': state_data[4],
                                                  }},
                                     '%CPU'     : state_data[5],
                                     'CPU_Mem'  : state_data[6],
                                     'T_CPU'    : state_data[7],
                                     '%GPU'     : state_data[8],
                                     'GPU_Mem'  : state_data[9],
                                     'T_GPU'    : state_data[10],
                                     'IB_speed' : state_data[11],
                                     'T_IB'     : state_data[12],
                                     'Disk'     : state_data[13],
                                    }
        elif len(state_data) == 4:
            node_state[node_name]['Job'][state_data[2]] = {'User'     : state_data[0],
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
    new_down = []
    for node in current_state:
        if not current_state[node]['State']  == 'Down':
            continue
        elif previous_state[node]['State'] == 'Down':
            continue
        else:
            new_down.append(node)
    return new_down

def check_CPU_temp(node_state, Temp_limit):
    high_temp_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['T_CPU']) > Temp_limit:
            high_temp_nodes[node] = node_state[node]['T_CPU']

    return high_temp_nodes

def check_GPU_temp(node_state, Temp_limit):
    high_temp_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['T_GPU']) > Temp_limit:
            high_temp_nodes[node] = node_state[node]['T_GPU']

    return high_temp_nodes
    
def check_IB_temp(node_state, IB_temp_limit):
    high_temp_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['T_IB']) > IB_temp_limit:
            high_temp_nodes[node] = node_state[node]['T_IB']

    return high_temp_nodes

def check_IB_speed(node_state, IB_speed_threshold):
    low_speed_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['IB_speed']) < IB_speed_threshold:
            low_speed_nodes[node] = node_state[node]['IB_speed']
    
    return low_speed_nodes

def check_disk_usage(node_state, Disk_usage_threshold):
    high_usage_nodes = {}
    for node in node_state:
        if node_state[node]['State'] == 'Down':
            continue
        if float(node_state[node]['Disk'][:-1]) > Disk_usage_threshold:
            high_usage_nodes[node] = node_state[node]['Disk']
    
    return high_usage_nodes

if __name__ == '__main__':
    # 0. Load node state files
    current_state = load_state(sys.argv[1])
    previous_state = load_state(sys.argv[2])
    
    for node in sorted(current_state):
        print(node, current_state[node])
    for node in sorted(current_state):
        print(node, previous_state[node])
    # 1. Node down
    new_down_nodes = check_node_down(current_state, previous_state)
    
    # 2. CPU high temperature
    CPU_temp_limit = 65.
    CPU_high_temp_nodes = check_CPU_temp(current_state, CPU_temp_limit)

    # 3. GPU high temperature
    GPU_temp_limit = 80.
    GPU_high_temp_nodes = check_GPU_temp(current_state, GPU_temp_limit)
    
    # 4. IB high temperature
    IB_temp_limit = 105.
    IB_high_temp_nodes = check_IB_temp(current_state, IB_temp_limit)

    # 5. IB speed incorrect
    IB_speed_threshold = 100.
    IB_low_speed_nodes = check_IB_speed(current_state, IB_speed_threshold)
    
    # 6. Disk volume uages
    Disk_usage_threshold = 80
    Disk_high_usage_nodes = check_disk_usage(current_state, Disk_usage_threshold)
        
    print new_down_nodes
    print CPU_high_temp_nodes
    print GPU_high_temp_nodes
    print IB_high_temp_nodes
    print IB_low_speed_nodes
    print Disk_high_usage_nodes
    # 7. Non-pbs job 
