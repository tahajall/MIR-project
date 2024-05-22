import time
import os
import json
import copy
from indexes_enum import Indexes
import tiered_index


class Index:
    def __init__(self, preprocessed_documents: list):
        """
        Create a class for indexing.
        """

        self.preprocessed_documents = preprocessed_documents

        self.index = {
            Indexes.DOCUMENTS.value: self.index_documents(),
            Indexes.STARS.value: self.index_stars(),
            Indexes.GENRES.value: self.index_genres(),
            Indexes.SUMMARIES.value: self.index_summaries(),
        }

    def index_documents(self):
        """
        Index the documents based on the document ID. In other words, create a dictionary
        where the key is the document ID and the value is the document.

        Returns
        ----------
        dict
            The index of the documents based on the document ID.
        """

        current_index = {}
        #
        for doc in self.preprocessed_documents:
            current_index.update({doc['id']:doc})

        return current_index

    def index_stars(self):
        """
        Index the documents based on the stars.

        Returns
        ----------
        dict
            The index of the documents based on the stars. You should also store each terms' tf in each document.
            So the index type is: {term: {document_id: tf}}
        """

        index_stars = {}
        for doc in self.preprocessed_documents:
            stars = doc['stars']
            stars_words = []
            if stars:
                for star in stars:
                    stars_words.extend(star.split())
            for word in set(stars_words):
                if word not in index_stars.keys():
                    index_stars.update({word:{doc['id']:stars_words.count(word)}})
                else:
                    index_stars[word].update({doc['id']:stars_words.count(word)})

        return index_stars

    def index_genres(self):
        """
        Index the documents based on the genres.

        Returns
        ----------
        dict
            The index of the documents based on the genres. You should also store each terms' tf in each document.
            So the index type is: {term: {document_id: tf}}
        """

        index_genres = {}
        for doc in self.preprocessed_documents:
            genres = doc['genres']
            genres_words = []
            if genres:
                for genre in genres:
                    genres_words.extend(genre.split())
            for word in set(genres_words):
                if word not in index_genres.keys():
                    index_genres.update({word: {doc['id']: genres_words.count(word)}})
                else:
                    index_genres[word].update({doc['id']: genres_words.count(word)})
        return index_genres

    def index_summaries(self):
        """
        Index the documents based on the summaries (not first_page_summary).

        Returns
        ----------
        dict
            The index of the documents based on the summaries. You should also store each terms' tf in each document.
            So the index type is: {term: {document_id: tf}}
        """

        current_index = {}
        for doc in self.preprocessed_documents:
            summaries = doc['summaries']
            summaries_words = []
            if summaries:
                for summary in summaries:
                    summaries_words.extend(summary.split())
            for word in set(summaries_words):
                if word not in current_index.keys():
                    current_index.update({word: {doc['id']: summaries_words.count(word)}})
                else:
                    current_index[word].update({doc['id']: summaries_words.count(word)})
        return current_index

    def get_posting_list(self, word: str, index_type: str):
        """
        get posting_list of a word

        Parameters
        ----------
        word: str
            word we want to check
        index_type: str
            type of index we want to check (documents, stars, genres, summaries)

        Return
        ----------
        list
            posting list of the word (you should return the list of document IDs that contain the word and ignore the tf)
        """

        try:
            if index_type == 'documents':
                return self.index[Indexes.DOCUMENTS.value][word].keys()
            if index_type == 'stars':
                return self.index[Indexes.STARS.value][word].keys()
            if index_type == 'genres':
                return self.index[Indexes.GENRES.value][word].keys()
            if index_type == 'summaries':
                return self.index[Indexes.SUMMARIES.value][word].keys()
        except:
            return []

    def add_document_to_index(self, document: dict):
        """
        Add a document to all the indexes

        Parameters
        ----------
        document : dict
            Document to add to all the indexes
        """

        doc_id = document['id']
        doc_genres = document['genres']
        doc_stars = document['stars']
        doc_summaries = document['summaries']

        self.index[Indexes.DOCUMENTS.value].update({doc_id:document})

        words = []
        for genre in doc_genres:
            words.extend(genre.split())
        genres_tf = {}
        for genre in set(words):
            genres_tf.update({genre:words.count(genre)})
        for genre in genres_tf.keys():
            if genre not in self.index[Indexes.GENRES.value]:
                self.index[Indexes.GENRES.value].update({genre:{document['id']:genres_tf[genre]}})
            else:
                if document['id'] not in self.index[Indexes.GENRES.value][genre].keys():
                    self.index[Indexes.GENRES.value][genre].update({document['id']:genres_tf[genre]})
                else:
                    self.index[Indexes.GENRES.value][genre][document['id']] += genres_tf[genre]

        words = []
        for star in doc_stars:
            words.extend(star.split())
        stars_tf = {}
        for star in set(words):
            stars_tf.update({star:words.count(star)})
        for star in stars_tf.keys():
            if star not in self.index[Indexes.STARS.value]:
                self.index[Indexes.STARS.value].update({star:{document['id']:stars_tf[star]}})
            else:
                if document['id'] not in self.index[Indexes.STARS.value][star].keys():
                    self.index[Indexes.STARS.value][star].update({document['id']:stars_tf[star]})
                else:
                    self.index[Indexes.STARS.value][star][document['id']] += stars_tf[star]

        words = []
        for summary in doc_summaries:
            words.extend(summary.split())
        summary_tf = {}
        for word in set(words):
            summary_tf.update({word:words.count(word)})
        for word in summary_tf.keys():
            if word not in self.index[Indexes.SUMMARIES.value]:
                self.index[Indexes.SUMMARIES.value].update({word:{document['id']:summary_tf[word]}})
            else:
                if document['id'] not in self.index[Indexes.SUMMARIES.value][word].keys():
                    self.index[Indexes.SUMMARIES.value][word].update({document['id']:summary_tf[word]})
                else:
                    self.index[Indexes.SUMMARIES.value][word][document['id']] += summary_tf[word]



    def remove_document_from_index(self, document_id: str):
        """
        Remove a document from all the indexes

        Parameters
        ----------
        document_id : str
            ID of the document to remove from all the indexes
        """

        if document_id in self.index[Indexes.DOCUMENTS.value].keys():
            self.index[Indexes.DOCUMENTS.value].pop(document_id)
        for star in self.index[Indexes.STARS.value]:
            if document_id in self.index[Indexes.STARS.value][star]:
                self.index[Indexes.STARS.value][star].pop(document_id)
        for genre in self.index[Indexes.GENRES.value]:
            if document_id in self.index[Indexes.GENRES.value][genre]:
                self.index[Indexes.GENRES.value][genre].pop(document_id)
        for word in self.index[Indexes.SUMMARIES.value]:
            if document_id in self.index[Indexes.SUMMARIES.value][word]:
                self.index[Indexes.SUMMARIES.value][word].pop(document_id)


    def check_add_remove_is_correct(self):
        """
        Check if the add and remove is correct
        """

        dummy_document = {
            'id': '100',
            'stars': ['tim', 'henry'],
            'genres': ['drama', 'crime'],
            'summaries': ['good']
        }

        index_before_add = copy.deepcopy(self.index)
        self.add_document_to_index(dummy_document)
        index_after_add = copy.deepcopy(self.index)

        if index_after_add[Indexes.DOCUMENTS.value]['100'] != dummy_document:
            print('Add is incorrect, document')
            return

        if (set(index_after_add[Indexes.STARS.value]['tim']).difference(set(index_before_add[Indexes.STARS.value]['tim']))
                != {dummy_document['id']}):
            print('Add is incorrect, tim')
            return

        if (set(index_after_add[Indexes.STARS.value]['henry']).difference(set(index_before_add[Indexes.STARS.value]['henry']))
                != {dummy_document['id']}):
            print('Add is incorrect, henry')
            return
        if (set(index_after_add[Indexes.GENRES.value]['drama']).difference(set(index_before_add[Indexes.GENRES.value]['drama']))
                != {dummy_document['id']}):
            print('Add is incorrect, drama')
            return

        if (set(index_after_add[Indexes.GENRES.value]['crime']).difference(set(index_before_add[Indexes.GENRES.value]['crime']))
                != {dummy_document['id']}):
            print('Add is incorrect, crime')
            return

        if (set(index_after_add[Indexes.SUMMARIES.value]['good']).difference(set(index_before_add[Indexes.SUMMARIES.value]['good']))
                != {dummy_document['id']}):
            print('Add is incorrect, good')
            return

        print('Add is correct')

        self.remove_document_from_index('100')
        index_after_remove = copy.deepcopy(self.index)

        if index_after_remove == index_before_add:
            print('Remove is correct')
        else:
            print('Remove is incorrect')

    def store_index(self, path: str, index_type: str = None):
        """
        Stores the index in a file (such as a JSON file)

        Parameters
        ----------
        path : str
            Path to store the file
        index_type: str or None
            type of index we want to store (documents, stars, genres, summaries)
            if None store tiered index
        """

        if not os.path.exists(path):
            os.makedirs(path)

        if index_type is None:

            tiered_index.Tiered_index(path)
        else:
            if index_type not in self.index :
                raise ValueError('Invalid index type')

            path = path + index_type  + "_index.json"
            with open(path, "w") as file:
                json.dump(self.index[index_type], file, indent=4)

    def load_index(self, path: str, index_type: str = None):
        """
        Loads the index from a file (such as a JSON file)

        Parameters
        ----------
        path : str
            Path to load the file
        """

        if not os.path.exists(path):
            os.makedirs(path)

        if index_type is None:
            with open(path, 'r') as f:
                self.index = json.load(f)
                f.close()

        if index_type not in self.index:
            raise ValueError('Invalid index type')

        if index_type == 'documents':
            with open(path, 'r') as f:
                self.index[Indexes.DOCUMENTS.value] = json.load(f)
                f.close()
        if index_type == 'stars':
            with open(path, 'r') as f:
                self.index[Indexes.STARS.value] = json.load(f)
                f.close()
        if index_type == 'genres':
            with open(path, 'r') as f:
                self.index[Indexes.GENRES.value] = json.load(f)
                f.close()
        if index_type == 'summaries':
            with open(path, 'r') as f:
                self.index[Indexes.SUMMARIES.value] = json.load(f)
                f.close()


    def check_if_index_loaded_correctly(self, index_type: str, loaded_index: dict):
        """
        Check if the index is loaded correctly

        Parameters
        ----------
        index_type : str
            Type of index to check (documents, stars, genres, summaries)
        loaded_index : dict
            The loaded index

        Returns
        ----------
        bool
            True if index is loaded correctly, False otherwise
        """

        return self.index[index_type] == loaded_index

    def check_if_indexing_is_good(self, index_type: str, check_word: str = 'good'):
        """
        Checks if the indexing is good. Do not change this function. You can use this
        function to check if your indexing is correct.

        Parameters
        ----------
        index_type : str
            Type of index to check (documents, stars, genres, summaries)
        check_word : str
            The word to check in the index

        Returns
        ----------
        bool
            True if indexing is good, False otherwise
        """

        # brute force to check check_word in the summaries
        start = time.time()
        docs = []
        for document in self.preprocessed_documents:
            if index_type not in document or document[index_type] is None:
                continue

            for field in document[index_type]:
                if check_word in field:
                    docs.append(document['id'])
                    break

            # if we have found 3 documents with the word, we can break
            if len(docs) == 3:
                break

        end = time.time()
        brute_force_time = end - start

        # check by getting the posting list of the word
        start = time.time()
        # based on your implementation, you may need to change the following line
        posting_list = self.get_posting_list(check_word, index_type)

        end = time.time()
        implemented_time = end - start

        print('Brute force time: ', brute_force_time)
        print('Implemented time: ', implemented_time)

        if set(docs).issubset(set(posting_list)):
            print('Indexing is correct')

            if implemented_time <= brute_force_time:
                print('Indexing is good')
                return True
            else:
                print('Indexing is bad')
                return False
        else:
            print('Indexing is wrong')
            return False

# TODO: Run the class with needed parameters, then run check methods and finally report the results of check methods
