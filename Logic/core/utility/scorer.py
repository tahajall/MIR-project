import math

import numpy as np

class Scorer:    
    def __init__(self, index, number_of_documents):
        """
        Initializes the Scorer.

        Parameters
        ----------
        index : dict
            The index to score the documents with.
        number_of_documents : int
            The number of documents in the index.
        """

        self.index = index
        self.idf = {}
        self.N = number_of_documents

    def get_list_of_documents(self,query):
        """
        Returns a list of documents that contain at least one of the terms in the query.

        Parameters
        ----------
        query: List[str]
            The query to be scored

        Returns
        -------
        list
            A list of documents that contain at least one of the terms in the query.
        
        Note
        ---------
            The current approach is not optimal but we use it due to the indexing structure of the dict we're using.
            If we had pairs of (document_id, tf) sorted by document_id, we could improve this.
                We could initialize a list of pointers, each pointing to the first element of each list.
                Then, we could iterate through the lists in parallel.
            
        """
        list_of_documents = []
        for term in query:
            if term in self.index.keys():
                list_of_documents.extend(self.index[term].keys())
        return list(set(list_of_documents))
    
    def get_idf(self, term):
        """
        Returns the inverse document frequency of a term.

        Parameters
        ----------
        term : str
            The term to get the inverse document frequency for.

        Returns
        -------
        float
            The inverse document frequency of the term.
        
        Note
        -------
            It was better to store dfs in a separate dict in preprocessing.
        """
        idf = self.idf.get(term, None)
        if idf is None:
            list_of_documents = []
            if term in self.index.keys():
                list_of_documents.extend(self.index[term].keys())
            df = len(list_of_documents)
            idf = math.log(self.N/df)
        return idf
    
    def get_query_tfs(self, query):
        """
        Returns the term frequencies of the terms in the query.

        Parameters
        ----------
        query : List[str]
            The query to get the term frequencies for.

        Returns
        -------
        dict
            A dictionary of the term frequencies of the terms in the query.
        """
        

        query_tfs = {term:query.count(term) for term in query}
        return query_tfs



    def compute_scores_with_vector_space_model(self, query, method):
        """
        compute scores with vector space model

        Parameters
        ----------
        query: List[str]
            The query to be scored
        method : str ((n|l)(n|t)(n|c).(n|l)(n|t)(n|c))
            The method to use for searching.

        Returns
        -------
        dict
            A dictionary of the document IDs and their scores.
        """


        method = method.split('.')
        doc_method = method[0]
        query_method = method[1]
        docs = self.get_list_of_documents(query)
        idfs = {term:self.get_idf(term) for term in query}
        query_tf = self.get_query_tfs(query)
        query_scores = []
        for term in query:
            term_tf = query_tf[term]
            if query_method[0] == 'l':
                term_tf = 1 + math.log(term_tf)
            term_idf = 1
            if query_method[1] == 't':
                term_idf = idfs[term]
            query_scores.append(term_idf*term_tf)
        query_scores = np.array(query_scores)
        if query_method[2] == 'c':
            weight = np.dot(query_scores,query_scores)
            query_scores = weight * query_scores
        query_score = {}
        for i,term in enumerate(query):
            query_score.update({term:query_scores[i]})


        scores = {}
        for doc in docs:
            doc_scores = []
            for term in query:
                doc_tf = self.index[term][doc]
                if doc_method[0] == 'l':
                    doc_tf = 1 + math.log(doc_tf)
                doc_idf = 1
                if doc_method[1] == 't':
                    doc_idf = idfs[term]
                doc_scores.append(doc_tf*doc_idf)
            doc_scores = np.array(doc_scores)
            if doc_method[2] == 'c':
                weight = np.dot(doc_scores,doc_scores)
                doc_scores = weight * doc_scores
            score = 0
            for i,term in enumerate(query):
                score += query_score[term] * doc_scores[i]
            scores.update({doc:score})


        return scores

    def get_vector_space_model_score(self, query, query_tfs, document_id, document_method, query_method):
        """
        Returns the Vector Space Model score of a document for a query.

        Parameters
        ----------
        query: List[str]
            The query to be scored
        query_tfs : dict
            The term frequencies of the terms in the query.
        document_id : str
            The document to calculate the score for.
        document_method : str (n|l)(n|t)(n|c)
            The method to use for the document.
        query_method : str (n|l)(n|t)(n|c)
            The method to use for the query.

        Returns
        -------
        float
            The Vector Space Model score of the document for the query.
        """

        #TODO
        pass

    def compute_socres_with_okapi_bm25(self, query, average_document_field_length, document_lengths):
        """
        compute scores with okapi bm25

        Parameters
        ----------
        query: List[str]
            The query to be scored
        average_document_field_length : float
            The average length of the documents in the index.
        document_lengths : dict
            A dictionary of the document lengths. The keys are the document IDs, and the values are
            the document's length in that field.
        
        Returns
        -------
        dict
            A dictionary of the document IDs and their scores.
        """

        # TODO
        pass

    def get_okapi_bm25_score(self, query, document_id, average_document_field_length, document_lengths):
        """
        Returns the Okapi BM25 score of a document for a query.

        Parameters
        ----------
        query: List[str]
            The query to be scored
        document_id : str
            The document to calculate the score for.
        average_document_field_length : float
            The average length of the documents in the index.
        document_lengths : dict
            A dictionary of the document lengths. The keys are the document IDs, and the values are
            the document's length in that field.

        Returns
        -------
        float
            The Okapi BM25 score of the document for the query.
        """

        # TODO
        pass

    def compute_scores_with_unigram_model(
            self, query, smoothing_method, document_lengths=None, alpha=0.5, lamda=0.5
    ):
        """
        Calculates the scores for each document based on the unigram model.

        Parameters
        ----------
        query : str
            The query to search for.
        smoothing_method : str (bayes | naive | mixture)
            The method used for smoothing the probabilities in the unigram model.
        document_lengths : dict
            A dictionary of the document lengths. The keys are the document IDs, and the values are
            the document's length in that field.
        alpha : float, optional
            The parameter used in bayesian smoothing method. Defaults to 0.5.
        lamda : float, optional
            The parameter used in some smoothing methods to balance between the document
            probability and the collection probability. Defaults to 0.5.

        Returns
        -------
        float
            A dictionary of the document IDs and their scores.
        """

        documents = self.get_list_of_documents(query)
        scores = {}
        for doc in documents:
            doc_score = self.compute_score_with_unigram_model(query,doc,smoothing_method,document_lengths,alpha,lamda)
            scores.update({doc:doc_score})
        return scores


    def compute_score_with_unigram_model(
            self, query, document_id, smoothing_method, document_lengths, alpha, lamda
    ):
        """
        Calculates the scores for each document based on the unigram model.

        Parameters
        ----------
        query : str
            The query to search for.
        document_id : str
            The document to calculate the score for.
        smoothing_method : str (bayes | naive | mixture)
            The method used for smoothing the probabilities in the unigram model.
        document_lengths : dict
            A dictionary of the document lengths. The keys are the document IDs, and the values are
            the document's length in that field.
        alpha : float, optional
            The parameter used in bayesian smoothing method. Defaults to 0.5.
        lamda : float, optional
            The parameter used in some smoothing methods to balance between the document
            probability and the collection probability. Defaults to 0.5.

        Returns
        -------
        float
            The Unigram score of the document for the query.
        """


        terms = query.split()
        score = 1
        T = np.sum(np.array(list(document_lengths.values())))
        for term in terms:
            if document_id in self.index[term]:
                doc_tf = self.index[term][document_id]
            else:
                doc_tf = 0
            term_cf = 0
            for doc in self.index[term].keys() :
                term_cf += self.index[term][doc]
            Ld = document_lengths[document_id]
            term_score = 0
            if smoothing_method == "bayes":
                term_score = (doc_tf+(alpha*term_cf/T))/(Ld + alpha)
            elif smoothing_method == "mixture":
                term_score = lamda*(doc_tf/Ld) + (1-lamda)*(term_cf/T)
            elif smoothing_method == "naive":
                term_score = doc_tf/Ld
            score *= term_score
        return score
