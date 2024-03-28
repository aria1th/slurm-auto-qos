import csv
import subprocess
from collections import defaultdict
partition_csv = "~/partitions.csv" # Partition Name,Allowed QoS Names,Node Names - comma separated values from sinfo result
result = subprocess.run(['sinfo'], stdout=subprocess.PIPE, check=True)
output = result.stdout.decode('utf-8')
lines = output.split('\n')
partition_list = {} # {"<status>" : {"<partition>" : ["<node>", "<node>", ...]}}

class NodeInfoParser:
    def __init__(self, node_name:str):
        self.node_name = node_name
        self.node_info = {}
        self.output = ""
        self.device_name = ""
        self.total_device_count = 0
        self.partitions = ""
        self.available_device_count = 0
    
    def get_command(self):
        return f"scontrol show node {self.node_name}".split()

    def get_node_info(self):
        result = subprocess.run(self.get_command(), stdout=subprocess.PIPE, check=True)
        self.output = result.stdout.decode('utf-8')
        # find Gres=<gres>
        gres_start = self.output.index('Gres=') + len('Gres=')
        gres_end = self.output.index('NodeAddr=') - 2
        gres = self.output[gres_start:gres_end]
        # split by ":"
        gres_found = False
        if gres != "(null)": # no Gres
            gres_found = True
        #print("gres", gres)
        self.node_info['gres'] = {}
        self.device_name = gres.split(":")[0]
        self.total_device_count = gres.split(':')[-1]
        # if not numeric self.total_device_count, check CfgTres=cpu=52,...
        if not self.total_device_count.isnumeric():
            cfg_start = self.output.index('CfgTRES=') + len('CfgTRES=')
            cfg_end = self.output.index('\n', cfg_start)
            cfg_tres = self.output[cfg_start:cfg_end]
            #print("cfg_tres", cfg_tres)
            # if gres/ in cfg_tres, use it
            if 'gres/' in cfg_tres:
                cfg_tres = cfg_tres.split('gres/')[-1]
            cfg_tres = cfg_tres.split(',')[0]
            self.node_info['gres'][self.device_name] = cfg_tres.split('=')[-1]
            self.gres_name = self.device_name
            self.available_device_count = self.total_device_count = self.node_info['gres'][self.device_name]
            assert self.available_device_count.isnumeric()
        # get AllocTRES
        alloc_start = self.output.index('AllocTRES=') + len('AllocTRES=')
        # until next \n
        alloc_end = self.output.index('\n', alloc_start)
        alloc_tres = self.output[alloc_start:alloc_end]
        if not alloc_tres or alloc_tres.isspace():
            # is idle
            # parse CfgTRES instead, gres/gpu=6\n
            cfg_start = self.output.index('CfgTRES=') + len('CfgTRES=')
            cfg_end = self.output.index('\n', cfg_start)
            cfg_tres = self.output[cfg_start:cfg_end]
            self.node_info['gres'][self.device_name] = cfg_tres.split('=')[-1]
            self.gres_name = self.device_name
            self.available_device_count = int(self.total_device_count)
        else:
            # split by ","
            alloc_tres_list = alloc_tres.split(',')
            any_gres_found = False
            for tres in alloc_tres_list:
                #print("tres", tres)
                if tres.startswith("gres/"):
                    tres = tres[len("gres/"):]
                    device_name = tres.split("=")[0]
                    count = tres.split('=')[-1]
                    self.node_info['gres'][device_name] = count
                    self.gres_name = device_name
                    self.available_device_count = int(self.total_device_count) - int(count)
                    any_gres_found = True
            if not any_gres_found:
                # if a=count format, just use it
                if '=' in alloc_tres:
                    self.node_info['gres'][self.device_name] = alloc_tres.split('=')[-1]
                    self.gres_name = self.device_name
                    self.available_device_count = int(self.total_device_count)
                else:
                    raise Exception(f"No Gres found in {self.node_name}! {alloc_tres_list}")
        # get Partitions
        partition_start = self.output.index('Partitions=') + len('Partitions=')
        partition_end = self.output.index('\n', partition_start)
        partitions = self.output[partition_start:partition_end]
        self.partitions = partitions
        return self.node_info
    
    def get_recommended_command(self, qos_name:str):
        self.get_node_info()
        if "null" in self.gres_name:
            return f"srun --partition={self.partitions} --time=72:0:0 --nodes=1 --node={self.node_name} --qos={qos_name.strip()} --pty bash -i"
        return f"srun --partition={self.partitions} --time=72:0:0 --nodes=1 --node={self.node_name} --qos={qos_name.strip()} --gres={self.gres_name}:{self.available_device_count} --pty bash -i"

class StringNodeParser:
    # node01 -> node01
    # node[01,03-05,07] -> node01,node03,node04,node05,node07
    # anode[01-03],bnode[01-03] -> anode01,anode02,anode03,bnode01,bnode02,bnode03
    # anode[01,03-05,07],bnode[01,03-05,07] -> anode01,anode03,anode04,anode05,anode07,bnode01,bnode03,bnode04,bnode05,bnode07
    def __init__(self, node_string):
        self.node_string = node_string
        self.node_list = []
        self.parse()
    def parse(self):
        # get "," which is not inside []
        comma_list = []
        inside_bracket = False
        for i, c in enumerate(self.node_string):
            if c == '[':
                inside_bracket = True
            elif c == ']':
                inside_bracket = False
            elif c == ',' and not inside_bracket:
                comma_list.append(i)
        # split by ","
        start = 0
        for i in comma_list:
            self.parse_node(self.node_string[start:i]) # anode[01-03] for example
            start = i + 1
        self.parse_node(self.node_string[start:])
    def parse_node(self, node_string):
        # if no bracket, just append
        if '[' not in node_string:
            self.node_list.append(node_string)
            return
        # parse bracket
        bracket_start = node_string.index('[')
        bracket_end = node_string.index(']')
        prefix = node_string[:bracket_start]
        # no suffix
        # if singleton inside bracket (no comma), just append
        if ',' not in node_string[bracket_start:bracket_end]:
            self.node_list.append(prefix + node_string[bracket_start:bracket_end + 1])
            return
        # we can safely assume that there is no nested bracket so just split by comma
        ranges = node_string[bracket_start + 1:bracket_end].split(',')
        for r in ranges:
            if '-' in r:
                start, end = r.split('-')
                for i in range(int(start), int(end) + 1):
                    self.node_list.append(prefix + str(i).zfill(len(start)))
            else:
                self.node_list.append(prefix + r)
    def get_node_list(self):
        return self.node_list

for line in lines:
    # PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
    if line.startswith('PARTITION'):
        continue
    if line == '':
        continue
    partiition, avail, timelimit, nodes, state, nodelist = line.split()
    # parse nodelist, nodename[01,03-05,07],... -> nodename01,nodename03,nodename04,nodename05,nodename07
    # check singleton node
    node_parser = StringNodeParser(nodelist)
    node_list = node_parser.get_node_list()
    if state not in partition_list:
        partition_list[state] = {}
    partition_list[state][partiition] = node_list
    
# print partition_list
# print list of idle partitions
print(f"Idle partitions: {list(partition_list.get('idle', {}).keys())} with nodes {list( partition_list.get('idle', {}).values())}")
# csv contains Partition Name,Allowed QoS Names
# read csv
idle_commands = []
mix_commands = []
with open(partition_csv, 'r') as csvfile:
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        partition_name, qos_list, _ = row
        qos_list = qos_list.split('|')
        if partition_name in partition_list.get('idle', {}):
            #print(f"Partition {partition_name} is idle with nodes {partition_list['idle'][partition_name]} and allowed QoS {qos_list}")
            # recommend srun --partition=suma_a100 --time=2:0 --nodes=1 --qos a100_qos --gres=gpu:1 --pty bash -i like command
            #idle_commands.append(f"srun --partition={partition_name} --time=2:0 --nodes=1 --qos={qos_list[-1]} --gres=gpu:1 --pty bash -i")
            idle_commands.append(NodeInfoParser(partition_list['idle'][partition_name][0]).get_recommended_command(qos_list[-1]))
            #print(NodeInfoParser(partition_list['idle'][partition_name][0]).get_recommended_command(qos_list[-1]))
        if partition_name in partition_list["mix"]:
            # get detailed info
            mix_commands.append(NodeInfoParser(partition_list["mix"][partition_name][0]).get_recommended_command(qos_list[-1]))
            #print(NodeInfoParser(partition_list["mix"][partition_name][0]).get_recommended_command(qos_list[-1]))
        # mix, get more detailed info
print("Idle commands:")
for c in idle_commands:
    print(c)
print("Mix commands:")
for c in mix_commands:
    print(c)
