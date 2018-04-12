#coding:utf-8
import sys
sys.path.append(".")
import os
reload(sys)  
sys.setdefaultencoding('utf8')

from practnlptools.tools import Annotator 
from nltk.chunk import conlltags2tree, tree2conlltags
from nltk.tokenize import sent_tokenize
from collections import defaultdict
from multiprocessing.dummy import Pool as ThreadPool
import json
from ast import literal_eval as make_tuple
from nltk.tree import Tree

annotator = Annotator()
#chunk sentence
def chunk_sent(sentence):
    try:
        annotations = annotator.getAnnotations(sentence) 
        iobtags=[(tuple(chunk)[0],tuple(pos)[1],tuple(chunk)[1].replace("S-","B-").replace("E-","I-")) for chunk,pos in zip(annotations['chunk'],annotations['pos'])]
        return str(iobtags)
    except IndexError, e:
        logger.error(e.args)

#sentence tokenizer
def token_sents(content):
    return sent_tokenize(content)


#chunk sentences
def chunk_content(content):
    result = []
    for i,sent in enumerate(token_sents(content)):
        result.append(chunk_sent(sent))
    # print result
    return result

def load_iobtags(iobtags):
    if iobtags is None:
        return None
    try:
        iobtags = [make_tuple(i.strip()) if i.endswith(')') else make_tuple(i.strip()+")") for i in iobtags[1:-1].split("),")]
        return conlltags2tree(iobtags)
    except:
        return None

def get_NPs(trees):
    NPs = []
    for i in range(len(trees)):
        tree = trees[i]
        if isinstance(tree,Tree) and tree.label()=="NP":
            wordseq,posseq = zip(*tree)
            NPs.append(" ".join(wordseq))
    return NPs


if __name__ == '__main__':
    print chunk_sent('I have an apple pen.')


















