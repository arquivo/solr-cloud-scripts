#!/bin/bash

usage() {
  cat << EOF >&2
Updates Solr Cloud to the latest configset 

Usage:
./send_config_set.sh SOLR_EXECUTABLE_PATH ZOOKEPER_HOST

Example:
./send_config_set.sh /opt/solr-8.11.2/bin/solr p44.arquivo.pt

IMPORTANT - Make sure the local solr executable is the same version as the one on solr cloud

EOF
  exit 1
}

if  [ $# -ne 2 ]; then
  usage;
fi

 $1 zk upconfig -n images -d solr-configset/images -z $2
