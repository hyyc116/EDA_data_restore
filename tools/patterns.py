#coding:utf-8

import sys
sys.path.append(".")

reload(sys)  
sys.setdefaultencoding('utf8')
from db_util import *
from nltk.chunk import conlltags2tree
from practnlptools.tools import Annotator 
from nltk.tokenize import sent_tokenize
from collections import defaultdict
from multiprocessing.dummy import Pool as ThreadPool
import json
import re
from nltk.tree import Tree
annotator = Annotator()

class logger:

    def __init__(self):
        self._index=0

    def step(self,step):
        self._index+=1

        if self._index%step==0:
            logging.info('progress:{:} ..'.format(self._index))

lg = logger()


#chunk sentence
def chunk_sent(sentence):
    try:
        annotations = annotator.getAnnotations(sentence) 
        iobtags=[(tuple(chunk)[0],tuple(pos)[1],tuple(chunk)[1].replace("S-","B-").replace("E-","I-")) for chunk,pos in zip(annotations['chunk'],annotations['pos'])]
        return iobtags
    except IndexError, e:
        logging.error(e.args)

def load_iobtags(iobtags):
    return conlltags2tree(iobtags)

#sentence tokenizer
def token_sents(content):
    return sent_tokenize(content)

#chunk sentences
def chunk_content(param):
    jid,content = param
    if content is None:
        return

    content = content.decode('utf-8',errors='ignore')
    sent = content
    NPs = []
    lg.step(1000)
    try:
        # print sent
        if '(' in sent:
            sent = sent.replace('(','')

        chunks = chunk_sent(sent)
        # print chunks
        trees = load_iobtags(chunks)
        # print trees
        nps = get_NPs(trees)
        # print nps
        NPs.extend(nps)
    except:
        logging.info('errors {:} ..')
        return
    # print result
    result = jid+"\t"+','.join(set(NPs))
    # print result
    print result


#chunk sentences
def chunk_file(path):

    if path is None:
        return
    # print path
    content = open(path).read()
    content = content.decode('utf-8',errors='ignore')
    if ":" not in content:
        return path.split('/')[-1][:-5]+"\t"
    content = content[content.index(':')+2:-2]
    content = content.strip().replace("\\u002F",'/')
    content = re.sub(r'<.*?>','',content).replace('\\n',' ')
    NPs = []
    lg.step(100)
    for sent in sent_tokenize(content):
        try:
            # print sent
            if '(' in sent:
                sent = sent.replace('(','')

            chunks = chunk_sent(sent)
            # print chunks
            trees = load_iobtags(chunks)
            # print trees
            nps = get_NPs(trees)
            # print nps
            NPs.extend(nps)
        except:
            logging.info('errors {:} ..')
            continue
    # print result
    result = path.split('/')[-1][:-5]+"\t"+','.join(set(NPs))
    # print result
    print result

def get_NPs(trees):
    NPs = []
    for i in range(len(trees)):
        tree = trees[i]
        if isinstance(tree,Tree) and tree.label()=="NP":
            wordseq,posseq = zip(*tree)
            NPs.append(" ".join(wordseq).strip())
    return NPs


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


if __name__ == '__main__':
    print chunk_file(sys.argv[1])




