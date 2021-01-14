from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from node import Node
import os

import datetime


START_NODE = "17274"
INFINITY = float("inf")

output_dir = 'output'
save_path = os.path.join(output_dir, 'shortest_path')
graph_file = '../data/ca-GrQc.txt'


def create_graph(line):
    current_node_id = line[0]
    neigbhours = line[1]
    node = Node(current_node_id)
    if current_node_id == START_NODE:
        node.dist = 0
        node.path = START_NODE

    for neighbor in neigbhours:
        n_node = Node(neighbor)
        node.add_neighbour(n_node)

    return (line[0], node)

def traverse_nodes(line):
    res = []
    node = line[1]

    if node.dist < INFINITY and (not node.visited):
        res.append((node.node_id, Node(node.node_id, node.dist, node.path, True)))

        for x in node.neighbours:
            x.dist = node.dist + 1
            x.path = node.path + "," + x.node_id
            res.append((x.node_id, x))

        return res
    else:
        res.append(line)

    return res


def update_nodes(a,b):
    main_node = None
    update_node = None

    if a.dist < b.dist:
        mainNode = b
        updateNode = a
        mainNode.dist = updateNode.dist
        mainNode.path = updateNode.path
    elif a.dist > b.dist:
        mainNode = a
        updateNode = b
        mainNode.dist = updateNode.dist
        mainNode.path = updateNode.path
    else:
        mainNode = a if len(a.neighbours) > 0 else b
    
    return mainNode

def format_result(line):
    dest_id = line[0]
    dest_node = line[1]

    path = dest_node.path
    cost = dest_node.dist

    return START_NODE + " | " + dest_id + " | " + str(path) + " | " + str(cost)
     
    

sc = SparkContext()
count = sc.accumulator(0)

graphRDD = sc.textFile(graph_file)

## Filter the comments
graphRDD = graphRDD.filter(lambda line:line[0] != "#")

## Create edge pairs
graphRDD = graphRDD.map(lambda line: (line.split("\t")[0],line.split("\t")[1]))

# Mode the graph as adjacency list
adjRDD = graphRDD.groupByKey().map(lambda line : create_graph(line))

while True:
    adjRDD = adjRDD.flatMap(lambda line: traverse_nodes(line))
    adjRDD = adjRDD.reduceByKey(lambda a,b:update_nodes(a,b))

    saveRDD = adjRDD.filter(lambda x: x[1].visited == False).cache()

    if (saveRDD.count() == count.value) and count.value != 1: 
        break

    count.value = saveRDD.count()

finalResultsRDD = adjRDD.map(lambda line: format_result(line))

# Results are partitioned into one machine to see the output in one file
finalResultsRDD.repartition(1).saveAsTextFile(save_path)

print(finalResultsRDD.take(20))
