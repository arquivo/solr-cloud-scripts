import requests
import concurrent.futures
import random
import math
import time
import random

import logging  

PARALLEL_REQUESTS = 5
WORDS_PER_QUERY = 2
WAIT = 0
ENDAT = 10

API_URL = "http://{}/imagesearch?q={}%20{}"
API_URL_ES = "http://{}/_search?q={}"

times = []

def read_word_list(word_list):
    alist = []
    with open(word_list) as f:
        for row in f:
            if not "-" in row:
                alist.append(row.strip())
    return alist

def generate_queries(word_list, words_per_query):
    random.shuffle(word_list)
    queries = []
    for i in range(math.ceil(len(word_list)/float(words_per_query))):
        queries.append(" ".join(word_list[i*words_per_query:(i+1)*words_per_query]))
    return queries


def make_request(query, host, duplicates="duplicates:off"):
    if "9200" in host:
        return make_request_es(query, host, duplicates)
    else:
        return make_request_solr(query, host, duplicates)


def make_request_solr(query, host, duplicates="duplicates:off"):
    elapsed = 0
    with requests.session() as s:
        s.keep_alive = False
        try:
           headers = {
              'Content-Type': 'application/json',
              'Connection': 'close',
           }
           r = s.get(API_URL.format(host, query, duplicates), headers=headers, stream=False)
           elapsed = r.elapsed.total_seconds()
           logging.debug("QUERY,{}".format(API_URL.format(host, query, duplicates)))
        except Exception as e:
           raise
        finally:
           s.close()
    
    return elapsed


def make_request_es(query, host, duplicates="duplicates:off"):
    elapsed = 0
    with requests.session() as s:
        s.keep_alive = False
        try:
           headers = {
              'Content-Type': 'application/json',
              'Connection': 'close',
           }
           r = s.get(API_URL_ES.format(host, query), headers=headers, stream=False)
           elapsed = r.elapsed.total_seconds()
           logging.debug("QUERY,{}".format(API_URL_ES.format(host, query)))
        except Exception as e:
           raise
        finally:
           s.close()
    
    return elapsed


def request_thread(queries, host, duration, duplicates="duplicates:off"):
    start_time = time.time()
    t_end = start_time + duration
    i = 0
    results = []
    out_of_time_requests = []
    while time.time() < t_end:
        req_time = make_request(queries[i%len(queries)], host, duplicates)
        if time.time() < t_end:
            results.append(req_time)
        else:
            out_of_time_requests.append(req_time)
        i+=1
    return results, out_of_time_requests


def request_thread_rps(query, host, duration, duplicates="duplicates:off"):
    global results
    results.append(make_request(query, host, duplicates))


def warmup(host, queries, n=50):
    global times
    for query in queries[:n]:
        make_request(query, host)


def test(host, queries, duration=60*5, parallel_requests=PARALLEL_REQUESTS, duplicates="duplicates:on"):
    start_time = time.time()
    output = []
    output_outoftime = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_requests) as e:
        results = []
        offset = len(queries)//parallel_requests
        for i in range(parallel_requests):
            queries_i = [q for q in queries[i*offset:(i+1)*offset]]
            results.append(e.submit(request_thread, queries_i, host, duration, duplicates))
        for r in results:
            results, out_of_time_requests = r.result()
            output.append(results)
            output_outoftime.append(out_of_time_requests)
    return output, output_outoftime

def test_rps(host, queries, duration=60*5, parallel_requests=PARALLEL_REQUESTS, duplicates="duplicates:on"):
    start_time = time.time()
    output = []
    output_outoftime = []
    final_time = time.time() + duration
    stime = time.time()
    while time.time() < final_time:
        stime = time.time()
        e = concurrent.futures.ThreadPoolExecutor(max_workers=parallel_requests)
        results = []
        offset = len(queries)//parallel_requests
        for i in range(parallel_requests):
            queries_i = [q for q in queries[i*offset:(i+1)*offset]]
            e.submit(request_thread, queries_i, host, duration, duplicates)
        sleep_time = time
        time.sleep(stime)
    return output, output_outoftime


if __name__ == '__main__':
    test('p51.arquivo.pt', [i for i in range(10000000)], 10, 3)
