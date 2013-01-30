package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper


@Log
class Marc2JsonLDConverter extends MarcCrackerAndLabelerIndexFormatConverter implements FormatConverter {

    String requiredContentType = "application/json"
    def marcmap

    Marc2JsonLDConverter() {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marcmap.json")
        mapper = new ElasticJsonMapper()
        this.marcmap = mapper.readValue(is, Map)
    }

    def createJson(URI identifier, Map injson) {
        def outjson = [:]
        outjson["@context"] = "http://libris.kb.se/contexts/libris.jsonld"
        outjson["@id"] = identifier.toString()
        // Workaround to prevent original data from being changed
        //outjson["marc21"] = mapper.readValue(mapper.writeValueAsBytes(injson), Map)
        injson = rewriteJson(identifier, injson)
        injson.fields.each {
            log.trace("Working on json field $it")
            it.each { fkey, fvalue ->
                if (fkey == "006" || fkey == "007" || fkey == "008") {
                    log.info("fvalue: " + fvalue["subfields"])
                    fvalue["subfields"].each {
                        it.each { skey, svalue ->
                            if (svalue.trim() && svalue != "|") {
                                outjson[skey] = svalue
                            }
                        }
                    }
                } else {

                }
            }
        }
        return outjson
    }


    @Override
    List<Document> convert(Document idoc) {
        outdocs = []
        if (doc.contentType == this.requiredContentType) {
            def injson = mapper.readValue(doc.dataAsString, Map)
            outdocs << new BasicDocument(doc).withData(mapper.writeValueAsBytes(createJson(doc.identifier, injson)))
        } else {
            log.warn("This converter requires $requiredContentType. Document ${doc.identifier} is ${doc.contentType}")
        }
        return outdocs
    }

    @Override
    List<Document> convert(List<Document> docs) {
        outdocs = []
        for (doc in docs) {
            outdocs << convert(doc)
        }
        return outdocs
    }
}
