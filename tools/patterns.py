#coding:utf-8

import sys
sys.path.append(".")

reload(sys)  
sys.setdefaultencoding('utf8')

from practnlptools.tools import Annotator 
from nltk.tokenize import sent_tokenize
from collections import defaultdict
from multiprocessing.dummy import Pool as ThreadPool
import json


annotator = Annotator()
logger = iters_logger(1,"Chunking corpus, counting sentences.")

def load_file(path):
    ids = path.split("_")[-3]
    content = open(path,"r",).read().strip()
    return ids,unicode(content, errors="ignore")


#chunk sentence
def chunk_sent(sentence):
    try:
        annotations = annotator.getAnnotations(sentence) 
        iobtags=[(tuple(chunk)[0],tuple(pos)[1],tuple(chunk)[1].replace("S-","B-").replace("E-","I-")) for chunk,pos in zip(annotations['chunk'],annotations['pos'])]
        return str(iobtags)
    except IndexError, e:
        logger.error(e.args)


def load_iobtags(iobtags):
    return conlltags2tree(iobtags)

#sentence tokenizer
def token_sents(content):
    return sent_tokenize(content)

#chunk sentences
def chunk_file(path):
    ids,content = load_file(path)

    result = []
    for i,sent in enumerate(token_sents(content)):
        logger.step()
        result.append((ids,i,chunk_sent(sent)))
    # print result
    return result

def chunk_folder(folder_path, index_path, start=0, end=-1, worker=4):
    
    logger.info("index path:{:}, start from {:} to end {:} with {:} workers".format(index_path,start,end,worker))
    chunk_results = defaultdict(dict)
    filelist = []
    for filepath in open(index_path):
        filepath=folder_path+"/el_files/"+filepath.strip()
        filelist.append(filepath)

    pool = ThreadPool(worker)
    results = pool.map(chunk_file, filelist[start:end])
    for result in results:
        for ids,i,chunks in result:
            chunk_results[ids][i]=chunks

    logger.info("saved chunk result to data/chunk_result.json.")
    open(folder_path+"/chunk_result.josn","w").write(json.dumps(chunk_results))
    logger.end()





op = sys.argv[1]
if op == "chunk_sentence":
    print chunk_sent("We can use an NLTK corpus module to access a larger amount of chunked text. ")
elif op=="chunk_file":
    for s in chunk_file(sys.argv[2]):
        print s
elif op == "chunk_folder":
    chunk_folder(sys.argv[2],sys.argv[3],start=int(sys.argv[4]),end=int(sys.argv[5]),worker=int(sys.argv[6]))
else:
    sys.stderr.write("No such operation: {:}\n".format(op))




