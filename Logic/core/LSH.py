import numpy as np
import itertools
import random


class MinHashLSH:
    def __init__(self, documents : list, num_hashes : int):
        """
        Initialize the MinHashLSH

        Parameters
        ----------
        documents : list of str
            The input documents for similarity analysis.
        num_hashes : int
            Number of hashes for mini-hashing.
        """
        self.documents = documents
        self.num_hashes = num_hashes

    def shingle_document(self, document, k=2) -> set:
        """
        Convert a document into a set of shingles.

        Parameters
        ----------
        document : str
            The input document.
        k : int
            The size of each shingle.

        Returns
        ----------
        set
            A set of shingles.
        """
        shingles = []
        word_list = document.split()
        for i in range(len(word_list)-k + 1) :
            shingle = ''
            for j in range(k):
                shingle += word_list[i+j] + " "
            shingles.append(shingle.strip())
        shingles = set(shingles)

        return shingles

    def build_characteristic_matrix(self):
        """
        Build the characteristic matrix representing the presence of shingles in documents.

        Returns
        ----------
        numpy.ndarray
            The binary characteristic matrix.
        """
        all_shingles = self.shingle_document(self.documents[0])
        for document in self.documents:
            all_shingles = all_shingles | self.shingle_document(document)
        matrix = np.zeros((len(all_shingles),len(self.documents)),dtype=int)
        all_shingles = list(all_shingles)
        for document in self.documents:
            for shingle in all_shingles:
                if shingle in document:
                    matrix[all_shingles.index(shingle),self.documents.index(document)] = 1
        return matrix

    def min_hash_signature(self):
        """
        Perform Min-Hashing to generate hash signatures for documents.

        Returns
        ----------
        numpy.ndarray
            The Min-Hash signatures matrix.
        """
        prime = 4294967311
        #prime = 7
        characteristic_matrix = self.build_characteristic_matrix()
        n , m = characteristic_matrix.shape
        hashes = [(random.randint(1,n),random.randint(1,n)) for i in range(self.num_hashes)]
        signature_matrix = [[np.inf for i in range(m)] for j in range(self.num_hashes)]
        signature_matrix = np.array(signature_matrix )
        for i in range(m):
            for j in range(len(hashes)):
                a , b = hashes[j]
                for k in range(n):
                    if characteristic_matrix[k,i] == 1 :
                        h = (a + k * b) % prime
                        if h < signature_matrix[j,i]:
                            signature_matrix[j,i] = h

        return signature_matrix.astype(int)

    def lsh_buckets(self, signature, bands=10, rows_per_band=10, number_of_buckets = 30):
        """
        Group documents into Locality-Sensitive Hashing (LSH) buckets based on Min-Hash signatures.

        Parameters
        ----------
        signature : numpy.ndarray
            Min-Hash signatures for documents.
        bands : int
            Number of bands for LSH.
        rows_per_band : int
            Number of rows per band.

        Returns
        ----------
        dict
            A dictionary mapping bucket IDs to lists of document indices.
        """
        #
        n , m = signature.shape
        subvectors = []
        for i in range(bands):
            subvectors.append(signature[i*rows_per_band: (i+1)*rows_per_band])
        subvectors = np.array(subvectors)
        buckets = {}
        for i in range(number_of_buckets):
            buckets.update({i:[]})
        for vector in subvectors:
            strings = []
            for i in range(m):
                strings.append(','.join(vector[:,i].astype(str)))
            for i in range(m):
                for j in range(i+1,m):
                    if strings[i] == strings[j]:
                        x = random.randint(0,number_of_buckets-1)
                        buckets[x].extend([i,j])

        return buckets

    def perform_lsh(self, number_of_bands=10, number_of_rows = 10, number_of_buckets = 10):
        """
        Perform the entire Locality-Sensitive Hashing (LSH) process.

        Returns
        ----------
        dict
            A dictionary mapping bucket IDs to lists of document indices.
        """
        #
        signature_matrix = self.min_hash_signature()
        return self.lsh_buckets(signature_matrix, bands=number_of_bands, rows_per_band=number_of_rows, number_of_buckets = number_of_buckets)

    def jaccard_score(self, first_set:set, second_set:set):
        """
        Calculate jaccard score for two sets.

        Parameters
        ----------
        first_set : set
            Set of first shingled document.
        second_set : set
            Set of second shingled document.

        Returns
        ----------
        float
            Jaccard score.
        """
        union = first_set | second_set
        intersection = first_set & second_set
        return len(intersection) / len(union)

    def jaccard_similarity_test(self, buckets, all_documents:list):
        """
        Test your near duplicate detection code based on jaccard similarity.

        Parameters
        ----------
        buckets : dict
            A dictionary mapping bucket IDs to lists of document indices.
        all_documents : list
            The input documents for similarity analysis.
        """
        correct_near_duplicates = 0
        all_near_duplicates = 0

        for bucket_id in buckets.keys():
            docs_in_this_bucket = buckets[bucket_id]
            unique_doc_ids = set(docs_in_this_bucket)
            if len(unique_doc_ids) > 1:
                combinations = list(itertools.combinations(unique_doc_ids, 2))
                for comb in combinations:
                    all_near_duplicates += 1

                    first_doc_id = comb[0]
                    second_doc_id = comb[1]

                    first_shingled_doc = self.shingle_document(all_documents[first_doc_id], 2)
                    second_shingled_doc = self.shingle_document(all_documents[second_doc_id], 2)

                    near_duplicated_jaccard_score = self.jaccard_score(first_shingled_doc, second_shingled_doc)
                    current_score = 0

                    for _ in range(5):
                        random_doc_id = first_doc_id
                        while random_doc_id == first_doc_id or random_doc_id == second_doc_id:
                            random_doc_id = random.randint(0, len(all_documents) - 1)
                        random_shingled_doc = self.shingle_document(all_documents[random_doc_id], 2)

                        random_jaccard_score = self.jaccard_score(first_shingled_doc, random_shingled_doc)

                        if near_duplicated_jaccard_score > random_jaccard_score:
                            current_score += 1

                    if current_score == 5:
                        correct_near_duplicates += 1

        # a good score is around 0.8
        print("your final score in near duplicate detection:", correct_near_duplicates / all_near_duplicates)

