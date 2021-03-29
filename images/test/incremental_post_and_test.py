import sys
import requests
import subprocess
import os
import json
import re

import time

import numpy as np

import random

import logging  
FORMAT = '%(asctime)-15s %(message)s'
logging.basicConfig(filename='times.log',level=logging.INFO, format=FORMAT)

from test_latency import test, warmup, read_word_list, generate_queries, WORDS_PER_QUERY


POST_LIMIT=10000000
URL_SPLIT_PATTERN = "[^\w\s\b]+";
#
# Usage:
#    ./post_and_log.sh p91.arquivo.pt SAFE Collections.txt
#
# Collections.txt has in each line the name of the collection to index
#
# Run inside a screen, this should be synchronous because we can only send images to Solr after the Indexing and Safe Classification
#

def run_test(HOST, SOLR_COLLECTION, PARALLEL_REQUESTS, EXPERIMENT_TIME, WARMUP_SIZE, NTESTS):
  word_list = read_word_list("palavras_warmup.txt")
  random.shuffle(word_list)
  queries_warmup_val = word_list[:WARMUP_SIZE]
  warmup(HOST, queries_warmup_val, WARMUP_SIZE)
  for i in range(NTESTS):
    for mode in ["duplicates:on"]:
      for parallel_request in PARALLEL_REQUESTS:
        time.sleep(60)
        logging.info("TEST,{},WARMUP".format(i))

        response = requests.get("http://{}/imagesearch?q=*%20duplicates:on".format(HOST, SOLR_COLLECTION))
        oresp = response.json()
        osize = oresp["totalItems"]

        word_list = read_word_list("palavras.txt")
        queries_test = generate_queries(word_list[WARMUP_SIZE:], WORDS_PER_QUERY)
        
        for wl, queries in [("palavras_warmup.txt", queries_test)]:
          logging.info("TEST_RUN,{},PARALLEL_REQUESTS,{}".format(i,parallel_request))
          logging.info("TEST_RUN,{},WORDLIST,{}".format(i,wl))
          logging.info("TEST_RUN,{},DUPLICATES,{}".format(i,mode))
          times_per_thread, outoftime_times_per_thread = test(HOST, queries, EXPERIMENT_TIME, parallel_request, mode)
          times = []
          for ind_times in times_per_thread:
            for t in ind_times:
              times.append(t)
          outoftime_times = []
          for ind_times in outoftime_times_per_thread:
            for t in ind_times:
              outoftime_times.append(t)
          
          requests_performed = len(times)
          requests_performed_out = len(outoftime_times)

          logging.info("TIMES,{},{},{},{},OK,{}".format(osize,parallel_request,mode,i,",".join([str(t) for t in times])))
          logging.info("TIMES,{},{},{},{},OUT,{}".format(osize,parallel_request,mode,i,",".join([str(t) for t in outoftime_times])))
          if times:
            times = np.array(times)
            logging.info("TEST_RUN,{},{},{},{},SIZE,{}".format(osize,parallel_request,mode,i,osize))
            logging.info("TEST_RUN,{},{},{},{},PARALLEL_REQUESTS,{}".format(osize,parallel_request,mode,i,parallel_request))
            logging.info("TEST_RUN,{},{},{},{},EXPERIMENT_TIME,{}".format(osize,parallel_request,mode,i,EXPERIMENT_TIME))
            logging.info("TEST_RUN,{},{},{},{},TOTAL_REQUESTS_INTIME,{}".format(osize,parallel_request,mode,i,requests_performed))
            logging.info("TEST_RUN,{},{},{},{},TOTAL_REQUESTS_OUTOFTIME,{}".format(osize,parallel_request,mode,i,requests_performed_out))
            logging.info("TEST_RUN,{},{},{},{},REQUESTS_PER_SEC,{}".format(osize,parallel_request,mode,i,requests_performed/EXPERIMENT_TIME))
            logging.info("TEST_RUN,{},{},{},{},MEAN,{}".format(osize,parallel_request,mode,i,times.mean()))
            logging.info("TEST_RUN,{},{},{},{},STD_DEV,{}".format(osize,parallel_request,mode,i,times.std()))
            logging.info("TEST_RUN,{},{},{},{},MIN_TIME,{}".format(osize,parallel_request,mode,i,times.min()))
            logging.info("TEST_RUN,{},{},{},{},MAX_TIME,{}".format(osize,parallel_request,mode,i,times.max()))
            logging.info("TEST_RUN,{},{},{},{},25TH_PERC,{}".format(osize,parallel_request,mode,i,np.percentile(times, 25)))
            logging.info("TEST_RUN,{},{},{},{},50TH_PERC,{}".format(osize,parallel_request,mode,i,np.median(times)))
            logging.info("TEST_RUN,{},{},{},{},75TH_PERC,{}".format(osize,parallel_request,mode,i,np.percentile(times, 75)))
            logging.info("TEST_RUN,{},{},{},{},95TH_PERC,{}".format(osize,parallel_request,mode,i,np.percentile(times, 95)))
            logging.info("TEST_RUN,{},{},{},{},99TH_PERC,{}".format(osize,parallel_request,mode,i,np.percentile(times, 99)))
          else:
            logging.info("TEST_RUN,NO_RESULTS_IN_TIME")


def post_and_test(HOST, SOLR_COLLECTION, PARALLEL_REQUESTS, WARMUP_SIZE, EXPERIMENT_TIME, RUNS_PER_SIZE, COLLECTION_LIST, SIZES, SOLR_HOST):
    OUT_TMP="/tmp/file.jsonl"
    SOLR_BIN="/opt/solr/bin/post"

    os.makedirs("log", exist_ok=True)

    logging.info("START,{}".format(time.time()))
    if not SIZES:
      run_test(HOST, SOLR_COLLECTION, PARALLEL_REQUESTS, EXPERIMENT_TIME, WARMUP_SIZE, RUNS_PER_SIZE)
    else:
      response = requests.get("http://{}/imagesearch?q=*%20duplicates:on".format(HOST, SOLR_COLLECTION))
      oresp = response.json()
      osize = int(oresp["totalItems"])

      with open(COLLECTION_LIST) as f:
        COLLECTION_LIST = [COLLECTION_FILE.strip() for COLLECTION_FILE in f]

      COLLECTION_FILE_I = 0
      indexed = osize

      for size in SIZES:
        tmp_file_len = 0
        out = open(OUT_TMP, "w")
        while indexed < size and COLLECTION_FILE_I < len(COLLECTION_LIST):
          COLLECTION_FILE = COLLECTION_LIST[COLLECTION_FILE_I]
          COLLECTION_FILE = COLLECTION_FILE.strip()
          logging.info("POST,COLLECTION,{}".format(COLLECTION_FILE))
          with open(COLLECTION_FILE) as file:
            for row in file:
              data = json.loads(row)

              if data["type"] == "page":
                #data["id"] = data["imgId"]
                for f in ["imgSrcBase64", "imgId", "type", "imgSurt" , "oldestSurt", "warcName", "warcOffset", "imgWarcName", "imgWarcOffset", "pageProtocol"]:
                  if f in data:
                    del data[f]

                data["imageMetadataChanges"] -= 1
                data["pageMetadataChanges"] -= 1
                data["pageUrlTokens"] = " ".join(re.split(URL_SPLIT_PATTERN, data["pageUrl"]))
                out.write(json.dumps(data) + "\n")
                indexed += 1
                tmp_file_len += 1
                if tmp_file_len == POST_LIMIT:
                  out.close()
                  logging.info("POST,RUNNING,{}".format(tmp_file_len))
                  subprocess.run("{} -host {} -c {} {}".format(SOLR_BIN, SOLR_HOST, SOLR_COLLECTION, OUT_TMP).split(" "))
                  out = open(OUT_TMP, "w")
                  tmp_file_len = 0
                if indexed >= size:
                  out.close()
                  logging.info("POST,RUNNING,{}".format(tmp_file_len))
                  subprocess.run("{} -host {} -c {} {}".format(SOLR_BIN, SOLR_HOST, SOLR_COLLECTION, OUT_TMP).split(" "))
                  tmp_file_len = 0
                  break
          #last file of the collection
          #posting must be stopped here to ensure documents that show up in more than one collection
          if "part-r-00149" in COLLECTION_FILE:
            out.close()
            logging.info("POST,RUNNING,{}".format(tmp_file_len))
            subprocess.run("{} -host {} -c {} {}".format(SOLR_BIN, SOLR_HOST, SOLR_COLLECTION, OUT_TMP).split(" "))
            out = open(OUT_TMP, "w")
            tmp_file_len = 0      
          COLLECTION_FILE_I += 1
          if indexed >= size:
            break
        
        if indexed < size:
          out.close()
          logging.info("POST,RUNNING,{}".format(tmp_file_len))
          subprocess.run("{} -host {} -c {} {}".format(SOLR_BIN, SOLR_HOST, SOLR_COLLECTION, OUT_TMP).split(" "))

        logging.info("POST,TEST,{},{}".format(size,indexed))
        run_test(HOST, SOLR_COLLECTION, PARALLEL_REQUESTS, EXPERIMENT_TIME, WARMUP_SIZE, RUNS_PER_SIZE)



if __name__ == "main":    
    HOST=sys.argv[1]             #e.g. p51.arquivo.pt
    SOLR_COLLECTION=sys.argv[2]  #e.g. SAFE: Solr collection name
    PARALLEL_REQUESTS=[int(a) for a in sys.argv[3].split(",")]  #e.g. PARALLEL_REQUESTS: num of parallel requests
    WARMUP_SIZE=int(sys.argv[4])  #e.g. WARMUP_SIZE: num of warmup queries
    EXPERIMENT_TIME=int(sys.argv[5])  #e.g. EXPERIMENT_TIME: experiment duration
    RUNS_PER_SIZE=int(sys.argv[6])  #e.g. RUNS_PER_SIZE: number of times to run the query time experiment
    SIZES=[]
    if len(sys.argv) > 7:
      COLLECTION_LIST=sys.argv[7] #e.g. text file with a list of jsonl files to post (one per line). jsonl files that match a collection must be together
      SIZES=[int(a) for a in sys.argv[8].split(",")] #e.g. sizes in which to run query time experiment
      SOLR_HOST=sys.argv[9] #e.g. solr host to post documents
    post_and_test(HOST, SOLR_COLLECTION, PARALLEL_REQUESTS, WARMUP_SIZE, EXPERIMENT_TIME, RUNS_PER_SIZE, COLLECTION_LIST, SIZES, SOLR_HOST)

