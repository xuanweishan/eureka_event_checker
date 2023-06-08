import sys

sys.dont_write_bytecode = True

import src.run_cli as rc

def get_all_jobs_in_all_nodes(alive_nodes):
    cmd = ['ps','axo', 'user:16,pid,%cpu,%mem,time,command']

    cmd_msg = {}
    jobs_in_all_nodes = {}

    rc.run_pdsh_cli(cmd, alive_nodes, cmd_msg)

    lines = cmd_msg['ps'].splitlines()

    for line in lines:
        data = line.split()
        node_name = data[0][:-1]
        if not data[0].startswith('eureka'):
            continue
        
        elif node_name not in jobs_in_all_nodes:
            jobs_in_all_nodes[node_name] = {}

        if data[1] in ['USER']:
            continue
        else:
            if data[1] not in jobs_in_all_nodes[node_name]:
                jobs_in_all_nodes[node_name][data[1]] = {}
        
        jobs_in_all_nodes[node_name][data[1]][data[2]] = {
                                                          '%CPU': data[3],
                                                          '%MEM': data[4],
                                                          'Time': data[5],
                                                          'Command': data[6:],
                                                         }
    return jobs_in_all_nodes

if __name__ == "__main__":
    # 1. Get alive nodes
    # 2. Get all user jobs from alive nodes
    all_jobs_in_all_nodes = get_all_jobs_in_all_nodes(alive_nodes)
    # 3. Print out jobs
    
