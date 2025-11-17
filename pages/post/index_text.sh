# Purpose: Indexes text files in Solr
#!/bin/bash

export JAVA_HOME="/opt/jdk-11.0.0.1/"
HOST=$3
PORT=$4
while IFS="" read -r p || [ -n "$p" ]
do
  for collection in $p
  do
    cat $collection | while read row 
      do
        echo /data/solr9/solr-9.6.1/bin/post -host $3 -port $4 -c $1 -type application/json $row 
        if  [[ "$row" == *"299"* ]]; then
          echo "commit"
          /data/solr9/solr-9.6.1/bin/post -host $3 -port $4 -c $1 -type application/json -commit yes -params "update.chain=script&overwrite=false" $row
        else
          /data/solr9/solr-9.6.1/bin/post -host $3 -port $4 -c $1 -type application/json -commit no  -params "update.chain=script&overwrite=false" $row
        fi
    done
  done
  #wget "http://$3:$4/solr/$1/update?commit=true"
  wget "http://$3:$4/solr/admin/cores?wt=json" -O "$p"_metrics.json
done < $2

