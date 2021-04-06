import requests
import sys
import json

PAGE_SIZE = 50000

host = sys.argv[1] #e.g. p44.arquivo.pt:8983
domains = sys.argv[2].split(",") #e.g. example.com,example.pt


for domain in domains:

	domain_filter = "pageHost:{0}*%20OR%20pageUrl:https\\:\\/\\/{0}*%20OR%20imgUrl:https\\:\\/\\/{0}*%20OR%20pageUrl:http\\:\\/\\/{0}*%20OR%20imgUrl:http\\:\\/\\/{0}*%20OR%20pageHost:www.{0}*%20OR%20pageUrl:https\\:\\/\\/www.{0}*%20OR%20imgUrl:https\\:\\/\\/www.{0}*%20OR%20pageUrl:http\\:\\/\\/www.{0}*%20OR%20imgUrl:http\\:\\/\\/www.{0}*".format(domain)

	base_query = "http://{}/solr/images/select?q=".format(host) + domain_filter
	r = requests.get(base_query)
	print(base_query)
	counts = r.json()["response"]["numFound"]

	pages = (counts//PAGE_SIZE) + 1
		
	for i in range(pages):
		print(counts-i*PAGE_SIZE)
		r = requests.get("{}&offset={}&fl=id&rows={}".format(base_query, i*PAGE_SIZE, PAGE_SIZE))
		d = [{"id": doc["id"], "blocked": {"set": 1}} for doc in r.json()["response"]["docs"]]
		requests.post("http://{}/solr/images/update?overwrite=true&commit=true".format(host), json=d)

