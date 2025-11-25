#Init config set on Zookeeper and create collection

#Tested on node p82, as it has the Solr binary

SOLR_BIN=/data/solr9/solr-9.6.1/bin/solr

ZH_HOST=p44.arquivo.pt

ZK_PORT=2201

SOLR_PORT=2200

NUM_SHARDS=12

REPLICATION_FACTOR=2

MAX_SHARDS_PER_NODE=12

CONFIG_NAME=pages

COLLECTION_NAME=page

git clone https://github.com/arquivo/arquivo-solr-tools.git

cd arquivo-solr-tools/pages/init/

$SOLR_BIN zk upconfig -n pages -d solr-configset/pages -z $ZH_HOST:$ZK_PORT

curl "http://$ZH_HOST:$SOLR_PORT/solr/admin/collections?action=CREATE&name=$COLLECTION_NAME&numShards=$NUM_SHARDS&replicationFactor=$REPLICATION_FACTOR&maxShardsPerNode=$MAX_SHARDS_PER_NODE&collection.configName=$CONFIG_NAME"
