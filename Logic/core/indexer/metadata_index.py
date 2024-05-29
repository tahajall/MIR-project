from .index_reader import Index_reader
from .indexes_enum import Indexes, Index_types
import json
import numpy as np

class Metadata_index:
    def __init__(self, path='index/'):
        """
        Initializes the Metadata_index.

        Parameters
        ----------
        path : str
            The path to the indexes.
        """
        

        self.path = path
        self.documents = None
        self.metadata_index = None
        self.read_documents()
        self.create_metadata_index()
        self.store_metadata_index(path)


    def read_documents(self):
        """
        Reads the documents.
        
        """

        with open(self.path + "preprocessed_data.json",'r') as f :
            self.documents = json.load(f)
            f.close()


    def create_metadata_index(self):    
        """
        Creates the metadata index.
        """
        metadata_index = {}
        metadata_index['averge_document_length'] = {
            'stars': self.get_average_document_field_length('stars'),
            'genres': self.get_average_document_field_length('genres'),
            'summaries': self.get_average_document_field_length('summaries')
        }
        metadata_index['document_count'] = len(self.documents)

        self.metadata_index = metadata_index
    
    def get_average_document_field_length(self,where):
        """
        Returns the sum of the field lengths of all documents in the index.

        Parameters
        ----------
        where : str
            The field to get the document lengths for.
        """

        lengths = []
        for doc in self.documents:
            if doc[where] :
                lengths.append(len(doc[where]))
            else:
                lengths.append(0)
        lengths = np.array(lengths)
        return np.mean(lengths)

    def store_metadata_index(self, path):
        """
        Stores the metadata index to a file.

        Parameters
        ----------
        path : str
            The path to the directory where the indexes are stored.
        """
        path =  path + Indexes.DOCUMENTS.value + '_' + Index_types.METADATA.value + '_index.json'
        with open(path, 'w') as file:
            json.dump(self.metadata_index, file, indent=4)


    
if __name__ == "__main__":
    meta_index = Metadata_index()