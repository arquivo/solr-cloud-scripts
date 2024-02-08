import sys
import requests
import subprocess
import os
import json
import re

import time

import random

import logging  

FORMAT = '%(asctime)-15s %(message)s'
SOLR_BIN = "/opt/solr-8.8.1/bin/post"

logging.basicConfig(filename='times.log',level=logging.INFO, format=FORMAT)

POST_LIMIT=10000000
URL_SPLIT_PATTERN = "[^\w\s\b]+";

def usage():
    print("""
Usage:

    python3.9 ./incremental_post.py SOLR_HOST SOLR_PORT SOLR_COLLECTION JSONL_LIST [OVERWRITE] 

Description:

    This script will post to Solr Cloud the results of the image indexing performed by hadoop. 
    When trying to post an image that was already indexed and posted into solr by another crawl, the 
    script will not overwrite previously indexed images. Instead, it will update the Solr document to 
    show the metadata of the older image, but also include the newer image's collection in the 
    collection list.
    To force the script to overwrite previously indexed images, the OVERWRITE flag must be set. 
    
Recommendations:
    - This script was made for python3.9, other versions of python were not tested and may or may not  
    cause the script to fail.
    - Before running the script, make sure this repository is on the latest version and that the 
    latest configset has been uploaded to Solr Cloud. The script send_config_set.sh was created for  
    this purpose.
    - Run this script inside of a screen, as it may take multiple hours to complete.

Parameters:

    SOLR_HOST       - The server that hosts Solr Cloud (e.g.: p44.arquivo.pt)
    SOLR_PORT       - The port that hosts Solr Cloud (e.g.: 8983)
    SOLR_COLLECTION - The name of the Solr collection to be posted to (e.g.: images)
    JSONL_LIST      - A text file containing the paths to the .jsonl files output by hadoop, one file  
                    path per line. (e.g.: toPost.txt)
    OVERWRITE       - Optional. The literal string 'OVERWRITE' (without quotes). If present, the script 
                    will delete previous entries of the same image rather than preserving the older one.
    """)



def post_and_log(SOLR_COLLECTION, COLLECTION_LIST, SOLR_HOST, SOLR_PORT, OVERWRITE):
    OUT_TMP="/tmp/file.jsonl"
    

    os.makedirs("log", exist_ok=True)

    logging.info("START,{},{},{},{},{},{}".format(time.time(), SOLR_COLLECTION, COLLECTION_LIST, SOLR_HOST, SOLR_PORT, OVERWRITE))

    response = requests.get("http://{}:{}/solr/{}/select/?q=*:*".format(SOLR_HOST, SOLR_PORT, SOLR_COLLECTION))
    oresp = response.json()
    osize = int(oresp["response"]["numFound"])

    with open(COLLECTION_LIST) as f:
      COLLECTION_LIST = [COLLECTION_FILE.strip() for COLLECTION_FILE in f]

    COLLECTION_LIST = sorted(COLLECTION_LIST)

    COLLECTION_FILE_I = 0
    indexed = osize

    tmp_file_len = 0
    out = open(OUT_TMP, "w")
    while COLLECTION_FILE_I < len(COLLECTION_LIST):
      COLLECTION_FILE = COLLECTION_LIST[COLLECTION_FILE_I]
      COLLECTION_FILE = COLLECTION_FILE.strip()
      logging.info("POST,COLLECTION,{}".format(COLLECTION_FILE))
      with open(COLLECTION_FILE) as file:
        for row in file:
          data = json.loads(row)

          if data["type"] == "page":
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
              subprocess.run("{} -Dparams=overwrite={} -host {} -c {} {}".format(SOLR_BIN, str(OVERWRITE).lower(), SOLR_HOST, SOLR_COLLECTION, OUT_TMP).split(" "))
              out = open(OUT_TMP, "w")
              tmp_file_len = 0
      #last file of the collection
      #posting must be stopped here to ensure documents that show up in more than one collection
      if "part-r-00149" in COLLECTION_FILE:
        out.close()
        logging.info("POST,RUNNING,{}".format(tmp_file_len))
        subprocess.run("{} -Dparams=overwrite={} -host {} -c {} {}".format(SOLR_BIN, str(OVERWRITE).lower(), SOLR_HOST, SOLR_COLLECTION, OUT_TMP).split(" "))
        out = open(OUT_TMP, "w")
        tmp_file_len = 0      
      COLLECTION_FILE_I += 1
    out.close()
    logging.info("POST,RUNNING,{}".format(tmp_file_len))
    subprocess.run("{} -Dparams=overwrite={} -host {} -c {} {}".format(SOLR_BIN, str(OVERWRITE).lower(), SOLR_HOST, SOLR_COLLECTION, OUT_TMP).split(" "))


if __name__ == "__main__":
    if len(sys.argv) < 5:
        usage()
        sys.exit()
    SOLR_HOST=sys.argv[1] #e.g. solr host to post documents
    SOLR_PORT=int(sys.argv[2]) #e.g. solr post
    SOLR_COLLECTION=sys.argv[3] #e.g. images: Solr collection name
    COLLECTION_LIST=sys.argv[4] #e.g. text file with a list of jsonl files to post (one per line). jsonl files that match a collection must be together
    OVERWRITE=False
    if len(sys.argv) > 5:
      OVERWRITE=(sys.argv[5].lower()=="overwrite") #USE WITH CARE: overwrite existing records if "overwrite" is passed as the fifth arg

    post_and_log(SOLR_COLLECTION, COLLECTION_LIST, SOLR_HOST, SOLR_PORT, OVERWRITE)

