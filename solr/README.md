# Custom Arquivo Apache Solr

Arquivo.pt Image and Page search requires a Solr with support for a Javascript engine.

From Java 12 the JavaScript engine has been removed.

We require that engine, because the insertion of data runs a JS file that in practice runs a deduplication, to prevent duplicated images or pages.

> Java 11 and previous versions come with a JavaScript engine called Nashorn, but Java 12 will require you to add your own JavaScript engine. Other supported scripting engines like JRuby, Jython, Groovy, all require you to add JAR files to Solr.

Reference: https://solr.apache.org/guide/solr/9_9/configuration-guide/script-update-processor.html#javascript

## Docker commands

```bash
docker build --target arquivo-solr -t arquivo-solr:test .
```

```bash
docker run --rm -it arquivo-solr:test bash
```

Build with changing solr version:

```bash
docker build --build-arg SOLR_VERSION=9.9.0 --target arquivo-solr -t arquivo-solr:test .
```
