import nltk
import re

class Preprocessor:

    def __init__(self, documents: list):
        """
        Initialize the class.

        Parameters
        ----------
        documents : list
            The list of documents to be preprocessed, path to stop words, or other parameters.
        """
        #nltk.download('punkt')
        #nltk.download('wordnet')
        #nltk.download('omw-1.4')
        self.documents = documents
        stopwords = []
        with open("stopwords.txt",'r') as f :
            for w in f:
                if w.endswith("\n"):
                    w = w.removesuffix("\n")
                stopwords.append(w)
            f.close()
        self.stopwords = stopwords

    def preprocess(self):
        """
        Preprocess the text using the methods in the class.

        Returns
        ----------
        List[str]
            The preprocessed documents.
        """
        preprocessed_documents = []
        if self.documents:
            for document in self.documents:
                pre_document = self.remove_links(document)
                pre_document = self.remove_punctuations(pre_document)
                pre_document = self.normalize(pre_document)
                preprocessed_documents.append(pre_document)
            return preprocessed_documents
        return self.documents

    def normalize(self, text: str):
        """
        Normalize the text by converting it to a lower case, stemming, lemmatization, etc.

        Parameters
        ----------
        text : str
            The text to be normalized.

        Returns
        ----------
        str
            The normalized text.
        """
        lemmatizer = nltk.stem.WordNetLemmatizer()
        tokenized_text = self.remove_stopwords(text)
        lemmatized_list = [lemmatizer.lemmatize(word) for word in tokenized_text]
        lemmatized_text = ' '.join(lemmatized_list)
        lemmatized_text = lemmatized_text.lower()
        return lemmatized_text

    def remove_links(self, text: str):
        """
        Remove links from the text.

        Parameters
        ----------
        text : str
            The text to be processed.

        Returns
        ----------
        str
            The text with links removed.
        """
        patterns = [r'\S*http\S*', r'\S*www\S*', r'\S+\.ir\S*', r'\S+\.com\S*', r'\S+\.org\S*', r'\S*@\S*']
        for pattern in patterns:
            text = re.sub(pattern,'',text)
        return text

    def remove_punctuations(self, text: str):
        """
        Remove punctuations from the text.

        Parameters
        ----------
        text : str
            The text to be processed.

        Returns
        ----------
        str
            The text with punctuations removed.
        """
        pattern = r'[^\w\s]'
        text = re.sub(pattern,'',text)
        return text

    def tokenize(self, text: str):
        """
        Tokenize the words in the text.

        Parameters
        ----------
        text : str
            The text to be tokenized.

        Returns
        ----------
        list
            The list of words.
        """
        tokens = nltk.word_tokenize(text)
        return tokens

    def remove_stopwords(self, text: str):
        """
        Remove stopwords from the text.

        Parameters
        ----------
        text : str
            The text to remove stopwords from.

        Returns
        ----------
        list
            The list of words with stopwords removed.
        """
        words = self.tokenize(text)
        clean_words = [ word for word in words if word not in self.stopwords ]
        return clean_words

