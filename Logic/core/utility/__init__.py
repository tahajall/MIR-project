from crawler import *
from evaluation import *
from preprocess import *
from scorer import *
from snippet import *
from spell_correction import *


#__all__ = [k for k in globals().keys() if not k.startswith("_")]
crawler = IMDbCrawler()
crawler.read_from_file_as_json()
for doc in crawler.crawled:
    pre = Preprocessor(doc['summaries'])
    doc['summaries'] = pre.preprocess()
    pree = Preprocessor(doc["stars"])
    doc["stars"] = pree.preprocess()
    preee = Preprocessor(doc["genres"])
    doc["genres"] = preee.preprocess()
with open("preprocessed_data.json", 'w') as f:
    json.dump(crawler.crawled, f)


