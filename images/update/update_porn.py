import requests
import sys
import json

PAGE_SIZE = 50000

host = sys.args[1] #e.g. p44.arquivo.pt:8983
domains = sys.args[2].split(",") #e.g. example.com,example.pt

domain_filter = "pageHost:" + "%20OR%20pageHost:".join(domains).strip()

base_query = "http://{}/solr/images/select?q=".format(host) + domain_filter
print(base_query)
r = requests.get(base_query)


counts = r.json()["response"]["numFound"]

pages = (counts//PAGE_SIZE) + 1
	
for i in range(pages):
	print(counts-i*PAGE_SIZE)
	r = requests.get("{}&offset={}&fl=id&rows={}".format(base_query, i*PAGE_SIZE, PAGE_SIZE))
	d = [{"id": doc["id"], "safe": {"set": 1}, "porn": {"set": 1}} for doc in r.json()["response"]["docs"]]
	requests.post("http://{}/solr/images/update?overwrite=true&commit=true".format(host), json=d)

