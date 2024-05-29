import json
import numpy as np
from utility.preprocess import Preprocessor
from utility.scorer import Scorer
from indexer.index import Indexes
from indexer.indexes_enum import Index_types
from indexer.index_reader import Index_reader


class SearchEngine:
    def __init__(self):
        """
        Initializes the search engine.

        """
        path = "C:/Users/ASUS/PycharmProjects/MIR-project/Logic/core/indexer/stored_index/"
        self.document_indexes = {
            Indexes.STARS: Index_reader(path, Indexes.STARS),
            Indexes.GENRES: Index_reader(path, Indexes.GENRES),
            Indexes.SUMMARIES: Index_reader(path, Indexes.SUMMARIES),
        }
        self.tiered_index = {
            Indexes.STARS: Index_reader(path, Indexes.STARS, Index_types.TIERED),
            Indexes.GENRES: Index_reader(path, Indexes.GENRES, Index_types.TIERED),
            Indexes.SUMMARIES: Index_reader(
                path, Indexes.SUMMARIES, Index_types.TIERED
            ),
        }
        self.document_lengths_index = {
            Indexes.STARS: Index_reader(
                path, Indexes.STARS, Index_types.DOCUMENT_LENGTH
            ),
            Indexes.GENRES: Index_reader(
                path, Indexes.GENRES, Index_types.DOCUMENT_LENGTH
            ),
            Indexes.SUMMARIES: Index_reader(
                path, Indexes.SUMMARIES, Index_types.DOCUMENT_LENGTH
            ),
        }
        self.metadata_index = Index_reader(
            path, Indexes.DOCUMENTS, Index_types.METADATA
        )


    def search(
        self,
        query,
        method,
        weights,
        safe_ranking=True,
        max_results=10,
        smoothing_method=None,
        alpha=0.5,
        lamda=0.5,
    ):
        """
        searches for the query in the indexes.

        Parameters
        ----------
        query : str
            The query to search for.
        method : str ((n|l)(n|t)(n|c).(n|l)(n|t)(n|c)) | OkapiBM25 | Unigram
            The method to use for searching.
        weights: dict
            The weights of the fields.
        safe_ranking : bool
            If True, the search engine will search in whole index and then rank the results.
            If False, the search engine will search in tiered index.
        max_results : int
            The maximum number of results to return. If None, all results are returned.
        smoothing_method : str (bayes | naive | mixture)
            The method used for smoothing the probabilities in the unigram model.
        alpha : float, optional
            The parameter used in bayesian smoothing method. Defaults to 0.5.
        lamda : float, optional
            The parameter used in some smoothing methods to balance between the document
            probability and the collection probability. Defaults to 0.5.

        Returns
        -------
        list
            A list of tuples containing the document IDs and their scores sorted by their scores.
        """
        preprocessor = Preprocessor([query])
        query = preprocessor.preprocess()[0]

        scores = {}
        if method == "unigram":
            scores = self.find_scores_with_unigram_model(
                query, smoothing_method, weights, scores, alpha, lamda
            )
        elif safe_ranking:
            scores = self.find_scores_with_safe_ranking(query, method, weights, scores)
        else:
            scores = self.find_scores_with_unsafe_ranking(
                query, method, weights, max_results, scores
            )

        final_scores = {}

        final_scores = self.aggregate_scores(weights, scores, final_scores)

        result = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)
        if max_results is not None:
            result = result[:max_results]

        return result

    def aggregate_scores(self, weights, scores, final_scores):
        """
        Aggregates the scores of the fields.

        Parameters
        ----------
        weights : dict
            The weights of the fields.
        scores : dict
            The scores of the fields.
        final_scores : dict
            The final scores of the documents.
        """

        for field in scores.keys():
            field_scores = scores[field]
            w = weights[field]
            for doc in field_scores.keys():
                field_scores[doc] *= w

        score1 = self.merge_scores(scores[Indexes.STARS],scores[Indexes.GENRES])
        final_scores = self.merge_scores(score1,scores[Indexes.SUMMARIES])
        return final_scores


    def find_scores_with_unsafe_ranking(
        self, query, method, weights, max_results, scores
    ):
        """
        Finds the scores of the documents using the unsafe ranking method using the tiered index.

        Parameters
        ----------
        query: List[str]
            The query to be scored
        method : str ((n|l)(n|t)(n|c).(n|l)(n|t)(n|c)) | OkapiBM25
            The method to use for searching.
        weights: dict
            The weights of the fields.
        max_results : int
            The maximum number of results to return.
        scores : dict
            The scores of the documents.
        """
        for field in weights:
            field_scores = {}
            for tier in ["first_tier", "second_tier", "third_tier"]:
                scorer = Scorer(self.tiered_index[field].index[tier],self.metadata_index.index['document_count'])
                if method == "okapiBM25":
                    field_scores.update( scorer.compute_socres_with_okapi_bm25(query, self.metadata_index.index[
                        "averge_document_length"][field.value], self.document_lengths_index[field].index))
                else:
                    field_scores.update( scorer.compute_scores_with_vector_space_model(query, method))
            scores.update({field:field_scores})
        return scores

    def find_scores_with_safe_ranking(self, query, method, weights, scores):
        """
        Finds the scores of the documents using the safe ranking method.

        Parameters
        ----------
        query: List[str]
            The query to be scored
        method : str ((n|l)(n|t)(n|c).(n|l)(n|t)(n|c)) | OkapiBM25
            The method to use for searching.
        weights: dict
            The weights of the fields.
        scores : dict
            The scores of the documents.
        """

        for field in weights:
            scorer = Scorer(self.document_indexes[field].index,int(self.metadata_index.index['document_count']))
            if method == "okapiBM25":
                field_scores = scorer.compute_socres_with_okapi_bm25(query,self.metadata_index.index["averge_document_length"][field.value],self.document_lengths_index[field].index)
            else:
                field_scores = scorer.compute_scores_with_vector_space_model(query,method)
            scores.update({field:field_scores})
        return scores

    def find_scores_with_unigram_model(
        self, query, smoothing_method, weights, scores, alpha=0.5, lamda=0.5
    ):
        """
        Calculates the scores for each document based on the unigram model.

        Parameters
        ----------
        query : str
            The query to search for.
        smoothing_method : str (bayes | naive | mixture)
            The method used for smoothing the probabilities in the unigram model.
        weights : dict
            A dictionary mapping each field (e.g., 'stars', 'genres', 'summaries') to its weight in the final score. Fields with a weight of 0 are ignored.
        scores : dict
            The scores of the documents.
        alpha : float, optional
            The parameter used in bayesian smoothing method. Defaults to 0.5.
        lamda : float, optional
            The parameter used in some smoothing methods to balance between the document
            probability and the collection probability. Defaults to 0.5.
        """
        #

        stars_scorer = Scorer(self.document_indexes[Indexes.STARS].index,int(self.metadata_index.index['document_count']))
        star_scores = stars_scorer.compute_scores_with_unigram_model(query,smoothing_method,self.document_lengths_index[Indexes.STARS].index,alpha,lamda)
        genres_scorer = Scorer(self.document_indexes[Indexes.GENRES].index,int(self.metadata_index.index['document_count']))
        genres_scores = genres_scorer.compute_scores_with_unigram_model(query,smoothing_method,self.document_lengths_index[Indexes.GENRES].index,alpha,lamda)
        summaries_scorer = Scorer(self.document_indexes[Indexes.SUMMARIES].index,int(self.metadata_index.index['document_count']))
        summaries_scores = summaries_scorer.compute_scores_with_unigram_model(query,smoothing_method,self.document_lengths_index[Indexes.SUMMARIES].index,alpha,lamda)
        scores = { Indexes.STARS:star_scores, Indexes.GENRES:genres_scores, Indexes.SUMMARIES:summaries_scores}
        return scores


    def merge_scores(self, scores1, scores2):
        """
        Merges two dictionaries of scores.

        Parameters
        ----------
        scores1 : dict
            The first dictionary of scores.
        scores2 : dict
            The second dictionary of scores.

        Returns
        -------
        dict
            The merged dictionary of scores.
        """
        if not scores1:
            if not scores2:
                return scores1
            return scores2


        scores1 = dict(sorted(scores1.items()))
        scores2 = dict(sorted(scores2.items()))
        merged_scores = {}
        for doc1 in scores1.keys():
            for doc2 in scores2.keys():
                if doc1 == doc2:
                    merged_scores.update({doc1:scores1[doc1]+scores2[doc2]})
                    break
                if doc1 < doc2:
                    merged_scores.update({doc1: scores1[doc1] })
                    break
                if doc2 < doc1:
                    merged_scores.update({doc2:scores2[doc2]})
        return merged_scores






if __name__ == "__main__":
    search_engine = SearchEngine()
    #print(search_engine.document_indexes[Indexes.STARS].index)
    #print(search_engine.metadata_index.index['document_count'])
    query = "spider man in wonderland"
    #method = "lnc.ltc"
    method = "unigram"
    smoothing_method = "bayes"
    weights = {Indexes.STARS: 1, Indexes.GENRES: 1, Indexes.SUMMARIES: 1}
    result = search_engine.search(query, method, weights, smoothing_method=smoothing_method)

    print(result)

