import networkx as nx

class LinkGraph:
    """
    Use this class to implement the required graph in link analysis.
    You are free to modify this class according to your needs.
    You can add or remove methods from it.
    """
    def __init__(self):
        self.nodes = {}
        self.number_of_edges = 0
        self.number_of_nodes = 0

    def add_edge(self, u_of_edge, v_of_edge):
        if v_of_edge not in self.get_successors(u_of_edge):
            u = self.nodes[u_of_edge]
            v = self.nodes[v_of_edge]
            u.add_successor(v)
            v.add_predecessor(u)
            self.number_of_edges += 1

    def add_node(self, node_to_add):
        if node_to_add not in self.nodes.keys():
            node = GraphNode(node_to_add)
            self.nodes.update({node_to_add:node})
            self.number_of_nodes += 1

    def get_successors(self, node):
        graph_node = self.nodes[node]
        successors = graph_node.successors
        successors = [successor.name for successor in successors]
        return successors

    def get_predecessors(self, node):
        graph_node = self.nodes[node]
        predecessors = graph_node.predecessors
        predecessors = [predecessor.name for predecessor in predecessors]
        return predecessors

class GraphNode:

    def __init__(self,name):
        self.name = name
        self.successors = []
        self.predecessors = []

    def add_successor(self,successor):
        self.successors.append(successor)

    def add_predecessor(self,predecessor):
        self.predecessors.append(predecessor)