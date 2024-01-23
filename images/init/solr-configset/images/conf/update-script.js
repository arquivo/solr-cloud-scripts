var MAX_FIELD_SIZE = 50

// https://stackoverflow.com/questions/9512084/how-can-i-modify-the-solr-update-handler-to-not-simply-overwrite-existing-docume
function removeDuplicates(arr) {
    var hash = {}, result = [];
    for ( var i = 0, l = arr.length; i < l; i++ ) {
        if ( !hash.hasOwnProperty(arr[i]) ) { //it works with objects! in FF, at least
            hash[ arr[i] ] = true;
            result.push(arr[i]);
        }
    }
    return result;
}

function processAdd(cmd) {
  var toRemove = ["type", "imgUrl", "imgUrlTokens", "pageTitle", "pageUrlTokens", "imgHeight", "imgWidth", "imgMimeType", "pageHost", "pageUrl", "isInline", "safe", "spam", "blocked", "drawing", "hentai", "neutral", "porn", "sexy", "imagesInOriginalPage"]
  var toRemoveTS = ["imgCrawlTimestamp", "pageCrawlTimestamp"]
  var toRemoveTSIfSmaller = ["imgCrawlTimestampLatest"]

  var toAddMultiValue = ["imgCaption", "imgAlt", "imgTitle", "collection"]

  var toSum = ["imageMetadataChanges", "pageMetadataChanges", "matchingImages", "matchingPages"]
 
  if (cmd.overwrite){
    return;
  }

  doc = cmd.solrDoc;
  var docId = doc.getFieldValue("id");

  
  var Term = Java.type("org.apache.lucene.index.Term");
  var TermObject = new Term("id", docId);

  var previousDocId = req.getSearcher().getFirstMatch(TermObject);
  
  //remove duplicates in incoming data
  toAddMultiValue.forEach(function(value) {
    try {
      values = doc.getFieldValues(value)
      values = removeDuplicates(values);
      doc.removeField(value)
      if (values)
        for (var i = 0; i < values.length; i++){
          if (i == MAX_FIELD_SIZE)
              break
          doc.addField(value, values[i])
        }

    } catch (error){
      //logger.info(error)
    }
  });


  //document exists
  if (previousDocId != -1) {


    var previousDoc = req.getSearcher().doc(previousDocId);
    var previousTimestamp = previousDoc.get("imgCrawlTimestamp")

    var currentTimestamp = Date.parse(doc.getFieldValue("imgCrawlTimestamp"))
    
    var parsedCollection = []
    for (var i = 0; i < previousDoc.getValues("collection").length; i++)
      parsedCollection.push("collection", previousDoc.getValues("collection")[i])

    var collection = doc.getFieldValue("collection")

    
    //incoming timestamp is older
    if (currentTimestamp < previousTimestamp){
      //logger.info("older: replace")
    } else { 
      //logger.info("newer: no replace")
      toRemove.forEach(function(field) {
        doc.removeField(field)
        doc.addField(field, previousDoc.get(field))
      });
      toRemoveTS.forEach(function(field) {
        var ts = new Date(Number(previousDoc.get(field))).toISOString()
        doc.removeField(field)
        doc.addField(field, ts)
      });
    }

    //collection was already parsed
    if (parsedCollection.indexOf(collection) != -1){
      //logger.info("collection parsed")
      toAddMultiValue.forEach(function(field) {
        doc.removeField(field)
      });
      toSum.forEach(function(field) {
        doc.removeField(field)
      });
    }

    // this code could be simpler, but as it is, it makes it obvious which values are added
    toAddMultiValue.forEach(function(field) {
      try {
        //values that are already in the index
        //NOTE: this is a Java array, not a JS array
        oldValues = previousDoc.getValues(field)
        //new values to add
        newValues = doc.getFieldValues(field)
        //remove all values in the new document to add, so we can add only the ones we want
        doc.removeField(field)
        var fieldSize = 0

        if (oldValues){
          //Prioritise adding values that are in the index already
          for (var i = 0; i < oldValues.length; i++){
              doc.addField(field, oldValues[i])
              fieldSize += 1
          }
        }
        //get a JS array "version" of oldValues
        oldValues = doc.getFieldValues(field)

        if (newValues){
          //Add the new ones if the field is not "full"
          for (var i = 0; i < newValues.length; i++){
            //check if value is in the index already
            if (oldValues.indexOf(newValues[i]) == -1){
              doc.addField(field, newValues[i])
              fieldSize += 1
            }
            if (fieldSize == MAX_FIELD_SIZE)
              break
          }
        }

      } catch (error){
        //logger.info(error)
      }
      //logger.info(doc.getFieldValue(field));
    });

    toSum.forEach(function(field) {
      var sumValue = Number(previousDoc.get(field)) + Number(doc.getFieldValue(field));
      doc.removeField(field)
      doc.addField(field, sumValue)
    });

    toRemoveTSIfSmaller.forEach(function(field) {
        var oldTs = new Date(Number(previousDoc.get(field)))
        var newTs = new Date(doc.getFieldValue(field))
        if (oldTs < newTs)
          ts = newTs.toISOString()
        else
          ts = oldTs.toISOString()
        doc.removeField(field)
        doc.addField(field, ts)
      });
  }
  cmd.overwrite = true
}

function processDelete(cmd) {
  // no-op
}

function processMergeIndexes(cmd) {
  // no-op
}

function processCommit(cmd) {
  // no-op
}

function processRollback(cmd) {
  // no-op
}

function finish() {
  // no-op
}
