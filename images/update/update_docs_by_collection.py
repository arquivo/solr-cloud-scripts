import requests
import sys
import json

PAGE_SIZE = 50000

host = sys.argv[1] #e.g. p44.arquivo.pt:8983
collections_to_filter = sys.argv[2].split(",") #e.g. EAWP31,EAWP33,EAWP34



other_collections = []
with open("1996-2019_collection.txt") as f:
	other_collections = [row.strip() for row in f]


fquery_string = "&fq=!collection:".join(other_collections)

query_string = "%20OR%20".join(["collection:{}".format(col) for col in collections_to_filter])

base_query = "http://{}/solr/images/select?q={}&fl=id&rows=0&fq=blocked:0&fq=!collection:{}".format(host, query_string, fquery_string)
r = requests.get(base_query)


counts = r.json()["response"]["numFound"]

pages = (counts//PAGE_SIZE) + 1
	
for i in range(pages):
	print(counts-i*PAGE_SIZE)
	r = requests.get("{}&offset={}&fl=id&rows={}".format(base_query, i*PAGE_SIZE, PAGE_SIZE))
	d = [{"id": doc["id"], "blocked": {"set": 0.5}} for doc in r.json()["response"]["docs"]]
	requests.post("http://{}/solr/images/update?overwrite=true&commit=true".format(host), json=d)

