from .graph import LinkGraph
from ..indexer.indexes_enum import Indexes
from ..indexer.index_reader import Index_reader

class LinkAnalyzer:
    def __init__(self, root_set):
        """
        Initialize the Link Analyzer attributes:

        Parameters
        ----------
        root_set: list
            A list of movie dictionaries with the following keys:
            "id": A unique ID for the movie
            "title": string of movie title
            "stars": A list of movie star names
        """
        self.root_set = root_set
        self.graph = LinkGraph()
        self.hubs = []
        self.authorities = []
        self.initiate_params()

    def initiate_params(self):
        """
        Initialize links graph, hubs list and authorities list based of root set

        Parameters
        ----------
        This function has no parameters. You can use self to get or change attributes
        """
        for movie in self.root_set:
            self.graph.add_node(movie["id"])
            self.hubs.append(movie["id"])
            for star in movie["stars"]:
                self.graph.add_node(star)
                self.authorities.append(star)
                self.graph.add_edge(movie["id"],star)


    def expand_graph(self, corpus):
        """
        expand hubs, authorities and graph using given corpus

        Parameters
        ----------
        corpus: list
            A list of movie dictionaries with the following keys:
            "id": A unique ID for the movie
            "stars": A list of movie star names

        Note
        ---------
        To build the base set, we need to add the hubs and authorities that are inside the corpus
        and refer to the nodes in the root set to the graph and to the list of hubs and authorities.
        """
        for movie in corpus:
            is_in_base_set = False
            for star in movie["stars"]:
                if star in self.authorities:
                    is_in_base_set = True
            if is_in_base_set:
                if movie["id"] not in self.hubs:
                    self.graph.add_node(movie["id"])
                    self.hubs.append(movie["id"])
                    for star in movie["stars"]:
                        self.graph.add_node(star)
                        self.graph.add_edge(movie["id"], star)
                        if star not in self.authorities:
                            self.authorities.append(star)



    def hits(self, num_iteration=5, max_result=10):
        """
        Return the top movies and actors using the Hits algorithm

        Parameters
        ----------
        num_iteration: int
            Number of algorithm execution iterations
        max_result: int
            The maximum number of results to return. If None, all results are returned.

        Returns
        -------
        list
            List of names of 10 actors with the most scores obtained by Hits algorithm in descending order
        list
            List of names of 10 movies with the most scores obtained by Hits algorithm in descending order
        """
        a_s = {}
        h_s = {}

        for hub in self.hubs:
            h_s.update({hub:1})
        for authority in self.authorities:
            a_s.update({authority:1})


        for i in range(num_iteration):
            for hub in self.hubs:
                successors = self.graph.get_successors(hub)
                sum = 0
                for successor in successors:
                    sum += a_s[successor]
                h_s[hub] = sum
            for authority in self.authorities:
                predecessors = self.graph.get_predecessors(authority)
                sum = 0
                for predecessor in predecessors:
                    sum += h_s[predecessor]
                a_s[authority] = sum

        a_s = sorted(a_s,reverse=True)
        h_s = sorted(h_s,reverse=True)


        return a_s[:max_result], h_s[:max_result]

if __name__ == "__main__":
    # You can use this section to run and test the results of your link analyzer
    corpus = []    # TODO: it shoud be your crawled data
    root_set = []   # TODO: it shoud be a subset of your corpus

    analyzer = LinkAnalyzer(root_set=root_set)
    analyzer.expand_graph(corpus=corpus)
    actors, movies = analyzer.hits(max_result=5)
    print("Top Actors:")
    print(*actors, sep=' - ')
    print("Top Movies:")
    print(*movies, sep=' - ')
