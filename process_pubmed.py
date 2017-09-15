import os, sys
import gzip
import datetime, time

from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=['localhost:9200'])
index_name = "pubmed-paper-index"
type_name = "pubmed-paper"

'''
Download the entire pubmed baseline with
wget -N ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/*.gz -P path/to/files

Get the daily updates with 
wget -N ftp://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/*.gz -P path/to/files
'''

def process_pubmed():

    print('Create pubmed paper es index... '),
    create_pubmed_paper_index()
    print 'done'
    
    # fill pubmed papers index
    pubmed_folder = '/Users/xflorian/Downloads/pubmed_data_2017/'
    # get a list of all .gz files in this folder
    list_of_files = [os.path.join(pubmed_folder, f) for f in os.listdir(pubmed_folder) if os.path.isfile(os.path.join(pubmed_folder, f)) and f[-2:] == 'gz']
    fill_pubmed_papers_table(list_of_files)

    pubmed_folder = '/Users/xflorian/Downloads/pubmed_data_2017_daily/'
    # get a list of all .gz files in this folder
    list_of_files = [os.path.join(pubmed_folder, f) for f in os.listdir(pubmed_folder) if os.path.isfile(os.path.join(pubmed_folder, f)) and f[-2:] == 'gz']
    fill_pubmed_papers_table(list_of_files)
    
    return 


def create_pubmed_paper_index():    
    settings = {
        # changing the number of shards after the fact is not possible max Gb per 
        # shard should be 30Gb, replicas can be produced anytime
        # https://qbox.io/blog/optimizing-elasticsearch-how-many-shards-per-index
        "number_of_shards" : 5,
        "number_of_replicas": 0
    }
    mappings = {
        "pubmed-paper": {
            "properties" : {
                "title": { "type": "string", "analyzer": "standard"},
                "abstract": { "type": "string", "analyzer": "standard"},
                "created_date": {
                    "type":   "date",
                    "format": "yyyy-MM-dd"
                }
            }
        }
    }
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, body={ 'settings': settings, 'mappings': mappings }, request_timeout=30)
    return 


def get_es_docs(paper):
    source = {
        "title": paper.title,
        "created_date": paper.created_datetime.date(),
        "abstract": paper.abstract
    }
    doc = {
        "index": {
            "_index": index_name,
            "_type": type_name,
            "_id": paper.pm_id
        }
    }
    return doc, source


import xml.etree.cElementTree as ET # C implementation of ElementTree
def fill_pubmed_papers_table(list_of_files):
    # Loop over all files, extract the information and index in bulk
    for i, f in enumerate(list_of_files):
        print "Read file %d filename = %s" % (i, f)
        time0 = time.time()
        time1 = time.time()
        inF = gzip.open(f, 'rb')
        # we have to iterate through the subtrees, ET.parse() would result
        # in memory issues
        context = ET.iterparse(inF, events=("start", "end"))
        # turn it into an iterator
        context = iter(context)

        # get the root element
        event, root = context.next()
        print "Preparing the file: %0.4fsec" % ((time.time() - time1))
        time1 = time.time()

        documents = []
        time1 = time.time()
        for event, elem in context:
            if event == "end" and elem.tag == "PubmedArticle":
                doc, source = extract_data(elem)
                documents.append(doc)
                documents.append(source)
                elem.clear()
        root.clear()
        print "Extracting the file information: %0.4fsec" % ((time.time() - time1))
        time1 = time.time()

        res = es.bulk(index=index_name, body=documents, request_timeout=300)
        es.indices.refresh(index=index_name)
        print "Indexing data: %0.4fsec" % ((time.time() - time1))
        print "Total time spend on this file: %0.4fsec\n" % ((time.time() - time0))
        #os.remove(f) # we directly remove all processed files
    return 


class Pubmed_paper():
    ''' Used to temporarily store a pubmed paper outside es '''
    def __init__(self):
        self.pm_id = 0
        self.created_datetime = datetime.datetime.today() # every paper has a created_date
        self.title = ""
        self.abstract = ""

    def __repr__(self):
        return '<Pubmed_paper %r>' % (self.pm_id)


def extract_data(citation):
    new_pubmed_paper = Pubmed_paper()

    citation = citation.find('MedlineCitation')

    new_pubmed_paper.pm_id = citation.find('PMID').text
    new_pubmed_paper.title = citation.find('Article/ArticleTitle').text

    Abstract = citation.find('Article/Abstract')
    if Abstract is not None:
        # Here we discart information about objectives, design, results and conclusion etc.
        for text in Abstract.findall('AbstractText'):
            if text.text:
                if text.get('Label'):
                    new_pubmed_paper.abstract += '<b>' + text.get('Label') + '</b>: '
                new_pubmed_paper.abstract += text.text + '<br>'

    DateCreated = citation.find('DateCreated')
    new_pubmed_paper.created_datetime = datetime.datetime(int(DateCreated.find('Year').text),\
                                                          int(DateCreated.find('Month').text),\
                                                          int(DateCreated.find('Day').text))

    doc, source = get_es_docs(new_pubmed_paper)
    del new_pubmed_paper
    return doc, source


def prettify(elem):
    from bs4 import BeautifulSoup # just for prettify
    '''Return a pretty-printed XML string for the Element.'''
    return BeautifulSoup(ET.tostring(elem, 'utf-8'), "xml").prettify()


if __name__ == "__main__":
    process_pubmed()
