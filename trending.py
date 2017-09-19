''' 
    This program finds the trending topics of pubmed based on the title and abstract
'''

import datetime
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors

from elasticsearch import Elasticsearch
es = Elasticsearch(hosts=['localhost:9200'])
index_name = "trending-pubmed-paper-index"
OUTPUT_FOLDER = ''

def get_doc(low_date, up_date, list_of_terms=[]):
    term_doc = []
    for sub_string in list_of_terms:
        term_doc.append({
            "match_phrase":{
                "title": sub_string
            }
        })
        term_doc.append({
            "match_phrase":{
                "abstract": sub_string
            }
        })
    doc = {
        "query": {
            "bool": {
                "must": [{
                    "range": {
                        "created_date":{
                            "gte" : low_date.strftime('%Y-%m-%d'), 
                            "lte" : up_date.strftime('%Y-%m-%d'), 
                            "format": "yyyy-MM-dd"
                        }
                    }
                },
                {
                    'bool': {
                        "should": term_doc
                    }
                }]
            }
        }
    }
    return doc


def get_paper_count(list_of_terms, timestep):
    # We start 25 years in the past
    start_date = datetime.datetime.utcnow() - datetime.timedelta(days=365*25)

    list_of_counts = []
    list_of_dates = []
    low_date = start_date
    # loop through the data year by year
    while low_date < datetime.datetime.utcnow() - datetime.timedelta(days=10):
        up_date = low_date + datetime.timedelta(timestep)

        doc = get_doc(low_date, up_date)
        # we are only interested in the count -> size=0
        res = es.search(index=index_name, size=0, body=doc) 
        norm = res['hits']['total']

        doc = get_doc(low_date, up_date, list_of_terms)
        res = es.search(index=index_name, size=10, body=doc) 

        # norm should always >0 but just in case   
        if norm > 0:
            list_of_counts.append(100.*float(res['hits']['total'])/float(norm))
            list_of_dates.append(low_date + datetime.timedelta(days=timestep/2))
        else:
            list_of_counts.append(0.)
            list_of_dates.append(low_date + datetime.timedelta(days=timestep/2))

        low_date = low_date + datetime.timedelta(timestep)
    return list_of_counts, list_of_dates


def create_trending_plot():
    timestep = 365 # average over 365 days
    
    # Get a generic list of colors
    colors = dict(mcolors.BASE_COLORS, **mcolors.CSS4_COLORS)
    colors = [color[0] for color in colors.items()]
    # get all possible line styles
    linestyles = ['-', '--', '-.', ':']

    list_of_queries = [['prostate cancer'], ['blood cancer', 'leukemia'], ['Ebola'],\
                       ['alzheimer', 'dementia']]
    timestamp = datetime.datetime.utcnow()

    plt.clf()
    # The maximum number of terms is given by the available colors
    for i, list_of_terms in enumerate(list_of_queries[:len(colors)]):
        print "i = ", i, "term = ", list_of_terms
        list_of_counts, list_of_dates = get_paper_count(list_of_terms, timestep)
        plt.plot(list_of_dates, list_of_counts, color=colors[i], label=', '.join(list_of_terms),\
                 linestyle=linestyles[i%len(linestyles)])
    plt.xlabel('Date [in steps of %d days]' % timestep)
    plt.title('Relative number of papers for topic vs. time')
    plt.ylabel('Relative number of papers [%]')
    plt.legend(loc='upper left', prop={'size': 7})
    plt.savefig(OUTPUT_FOLDER + "trending_pubmed_%s.png" % timestamp.strftime('%m-%d-%Y-%H-%M-%f'))
    return 

if __name__ == "__main__":
    create_trending_plot()
