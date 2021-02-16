import os
import shutil
import json
import re
import pandas as pd
import networkx
import traceback

src_path = "/Users/ed/writing/egretta/resources" # don't use input - it's builtin functiom
out_path = "../resources/jenkins_meta/"

def __get_nodename_on_pipeline(node_on_egretta, node_dict):
    nodename = node_dict[node_on_egretta]["node_name"]
    return nodename

def __get_location(type, node, input_nodename, jobname):
    nodename = None


    # CASE
    # source   jdbc/file/sql
    # process
    # sink     custom/file

    def remove_partition_subfolder(location):
        if location != None:
            if location[-1] == '/':
                location = location[:-1]

        def get_new_location(x):
            result = None
            if (('name=' in x) ):
                result =x
            elif (("latest" in x)
                  or ("date_id" in x)
                  or ("hour_id" in x)
                  or ('created_at' in x)
                  or ("created_at_date" in x)
                  or ("year" in x)
                  or ("month" in x)
                  or ("day" in x)
                  or ("hour" in x)):
                result = ''
            elif x.endswith('_compact'):
                result =  (x.replace('_compact',''))
            else:
                result = x
            return result



        new_location = '/'.join(map(lambda x: get_new_location(x), location.split('/')))
        # new_location = location

        # ^((?!hede).)*$
        # (?<!(regex2))regex1
        new_location = re.sub('(?<!(s3:))/+','/',new_location)
        return new_location

    if (type == "source"):

        if (node["type"] == "file"):
            _location = node["options"].get("paths",None)
            location = remove_partition_subfolder(_location)
            nodename = location
        elif (node["type"] == "jdbc"):
            suffix = node["options"].get("dbtable")
            nodename = ":".join(['jdbc',suffix])
        elif (node["type"] == "sql"):
            nodename = f"""sql_node:{node["name"]}"""
        else:
            nodename = node["name"]

    elif (type == "sink"):
        if (node["type"] == "file"):
            # cannot use node["name"] since it's "filesink"
            _location = node["options"].get("path",None)
            location = remove_partition_subfolder(_location)
            nodename = location
        elif (node["type"] == "custom") and (node["name"] == "file_sink"):
            _location = node["options"].get("path",None)
            location = remove_partition_subfolder(_location)
            nodename = location
        elif (node["type"] == "custom") and (node["name"] == "approval_sink"):
            _location = node["options"].get("path",None)
            location = remove_partition_subfolder(_location)
            nodename = location
        else:
            nodename = node["name"]

    elif (type == "process"):
        nodename = f"""process:::{node["name"]}:::{jobname}"""
    else:
        nodename = node["name"]
    return nodename

def extract_egretta_jobs():
    result = list()
    G1 = networkx.DiGraph()

    for filename in os.listdir(out_path):
        node_dict = {}
        with open(os.path.join(out_path,filename), 'r') as file:
            _meta = file.read()
            meta = json.loads(_meta)

            try:
                jobname = meta["name"]
            except Exception as e:
                continue

            for src_node in meta['source']:
                type = "source"
                node_name = __get_location(type, src_node, filename, jobname)
                node_attr = {"filename":filename, "options":src_node["options"], "role": type, "type":src_node["type"]}
                if G1.has_node(node_name):
                    G1.nodes[node_name]["options"].append(node_attr)
                else:
                    G1.add_node(node_name, options=[node_attr])
                node_dict[src_node["name"]] = {"node_name": node_name, "node_attr": node_attr}

            for proc_node in meta['process']:
                type = "process"
                node_name = __get_location(type, proc_node, filename, jobname)
                node_attr = {"filename":filename, "options":proc_node["options"], "role": type, "type":proc_node.get("type",None)}
                if (G1.has_node(node_name)):
                    G1.nodes[node_name]["options"].append(node_attr)
                else:
                    G1.add_node(node_name, options=[node_attr])

                for input_node in proc_node["inputs"]:
                    input_nodename = __get_nodename_on_pipeline(input_node, node_dict)
                    if (G1.has_node(input_nodename)):
                        G1.add_edge(input_nodename, node_name)
                    # else:
                    #     G1.add_node(input_nodename)
                        # G1.add_edge(input_nodename, node_name)
                node_dict[proc_node["name"]] = {"node_name": node_name, "node_attr": node_attr}

            for sink_node in meta['sink']:
                type = "sink"
                node_name = __get_location(type, sink_node, filename, jobname)
                node_attr = {"filename":filename, "options":sink_node["options"], "role": type, "type": sink_node["type"]}
                if (G1.has_node(node_name)):
                    # G1.add_node(node_name, options=sink_node["options"], location = location)
                    G1.nodes[node_name]["options"].append(node_attr)
                else:
                    G1.add_node(node_name, options=[node_attr])
                for input_node in sink_node["inputs"]:
                    input_nodename = __get_nodename_on_pipeline(input_node, node_dict)
                    if (G1.has_node(input_nodename)):
                        G1.add_edge(input_nodename, node_name)
                    # else:
                    #     G1.add_node(input_nodename)
                    #     G1.add_edge(input_nodename, node_name)

                # cannot use node["name"] since it has "file_sink"
                node_dict[node_name] = {"node_name": node_name, "node_attr": node_attr}

    edges = [x for x in G1.edges(data=False)] #if G1.out_degree(x)!=0 and G1.in_degree(x)==0]

    for edge in edges:
        result.append({
                "source":edge[0],
                "sink":edge[1],
                "filename": filename,
                "jobname":jobname,
                "src_attr":G1.nodes[edge[0]]["options"],
                "sink_attr":G1.nodes[edge[1]]["options"] })
    return result
