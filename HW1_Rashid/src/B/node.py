
# START_NODE = "17274"
INFINITY = float("inf")
class Node:
    """docstring for Node"""
    def __init__(self, node_id, dist = INFINITY, path = "", visited = False):
        self.node_id = node_id
        self.neighbours = []
        self.dist = dist
        self.path = path
        self.visited = visited

    def add_neighbour(self,node):
        self.neighbours.append(node)

    def __str__(self):
        return f"Node ID: | {self.node_id} | NEIGHBOURS: {[x.node_id for x in self.neighbours]} | DIST: {self.dist} | PATH: {self.path} | VISITED: {self.visited}"