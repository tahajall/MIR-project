import json
import preprocess
import LSH

documents = []
with open("LSHFakeData.json", 'r') as f:
    documents = json.load(f)
summaries = []
for doc in documents:
    summaries.append(doc['summaries'][0])

pre = preprocess.Preprocessor(summaries)
pre_documents = pre.preprocess()
lsh = LSH.MinHashLSH(pre_documents,100)
buckets = lsh.perform_lsh(number_of_bands=20,number_of_rows=5,number_of_buckets=50)
lsh.jaccard_similarity_test(buckets,pre_documents)
