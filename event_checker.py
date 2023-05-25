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

if __name__ == '__main__':
    # 0. Load node state files
    current_state = load_state(sys.argv[1])
    previous_state = load_state(sys.argv[2])
    
    for node in sorted(current_state):
        print(node, current_state[node])
    for node in sorted(current_state):
        print(node, previous_state[node])
    # 1. Node down
    

    # 2. CPU high temperature
    
    # 3. GPU high temperature
    
    # 4. IB high temperature
    
    # 5. IB speed incorrect
    
    # 6. Disk volume uages
    
    # 7. Non-pbs job 
