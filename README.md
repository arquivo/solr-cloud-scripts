
# Solr Cloud Scripts

Collects all scripts to be used to setup SolrCloud for images and text.

## Installation


Clone the repo.

There are no python requirements other than `numpy` for incremental_post_and_long

There is an external Solr dependency.
Use the same version as the version currently used in the search indexes (currently Solr 8.8.1)
Update script constants to setup the location of the Solr bin. 


## Image script description

### init

Used to init a SolrCloud image index.

- `solr-configset/`: contains the necessary information to make a SolrCloud instance ready to have an Arquivo.pt image search index. They are used in the `solr-cloud-image-index` Ansible roles, which is the prefered way for creating an image index. Relevant files include:
  - `images/conf/update-script.js`: script that takes care of deduplication of images across collections. It performs the same role as the `DupDigestMerger` job from the [image search indexer](https://github.com/arquivo/image-search-indexing).
  - `images/conf/managed-schema`: Solr schema for the current image index 
- `send_config_set.sh`: sends the `images` configset to Zookeeper


### post

Used to send documents to Solr.

- `incremental_post.py`: used to send the JSONL files created by the image indexing pipeline. More info on how this works [here](https://docs.google.com/document/d/1yTnbRZ4b3_Q5oFzgMqYV_BfjbTP6yKD8rQ1dbbD6MHE/)

### test

Used to test the capacity of a SolrCloud instance.

- `incremental_post_and_test.py`: script that sends a set number of documents to Solr (e.g. 1000000, 5000000) and measures how fast the retrieval is. It used to estimate the maximum capacity of a server, in terms of number of documents indexed vs. retrieval time.
- `test_latency.py`: aux functions to measure latency.
- `WorkBench.jmx`: JMeter script to measure 
- `queries.txt`: quereis generated by random pairs of words

### update

Example scripts on how to manipulate documents in the Solr index after posting

- `update_block.py`: update documents according to the block list.
- `update_docs_by_collection.py`: re-embargo collections 
- `update_nsfw.py`: update nsfw status of a set of documents
