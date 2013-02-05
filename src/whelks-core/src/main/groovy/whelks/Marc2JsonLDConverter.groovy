package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper


@Log
class Marc2JsonLDConverter extends MarcCrackerAndLabelerIndexFormatConverter implements FormatConverter {

    String requiredContentType = "application/json"
    def marcref

    Marc2JsonLDConverter() {
        mapper = new ObjectMapper()
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marc_refs.json")
        this.marcref = mapper.readValue(is, Map)
    }

    def createJson(URI identifier, Map injson) {
        def outjson = [:]
        def pfx = identifier.toString().split("/")[1]
        outjson["@context"] = "http://libris.kb.se/contexts/libris.jsonld"
        outjson["@id"] = identifier.toString()
        // Workaround to prevent original data from being changed
        //outjson["marc21"] = mapper.readValue(mapper.writeValueAsBytes(injson), Map)
        injson = rewriteJson(identifier, injson)
        log.trace("Leader: ${injson.leader}")
        injson.leader.subfields.each { 
            it.each { lkey, lvalue ->
                lvalue = lvalue.trim()
                if (lvalue && !(lvalue =~ /^\|+$/)) {
                    outjson[lkey] = lvalue
                }
            }
        }
        injson.fields.each {
            log.trace("Working on json field $it")
            it.each { fkey, fvalue ->
                if ((fkey as int) > 5 && (fkey as int) < 9) {
                    fvalue["subfields"].each {
                        it.each { skey, svalue ->
                            svalue = svalue.trim()
                            if (svalue && !(svalue =~ /^\|+$/)) {
                                outjson[skey] = svalue
                            }
                        }
                    }
                } else {
                    log.trace("Value: $fvalue")
                    if (marcref[pfx][fkey]) {
                        log.trace("Found a reference: " +marcref[pfx][fkey])
                        fvalue["subfields"].each {
                            it.each { skey, svalue ->
                                def label  = marcref[pfx][fkey][skey]
                                def linked = marcref[pfx][fkey]["_linked"]
                                if (linked) {
                                    log.trace("Create new entity for $fkey")
                                    createEntity(fvalue)
                                } else if (label) {
                                    if (outjson[label]) {
                                        log.trace("Adding $svalue to outjson")
                                        if (outjson[label] instanceof List) {
                                            outjson[label] << svalue
                                        } else {
                                            def l = []
                                            l << outjson[label]
                                            l << svalue
                                            outjson[label] = l
                                        }
                                    } else {
                                        log.trace("Inserting $svalue in outjson")
                                        outjson[label] = svalue
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return outjson
    }

    def createEntity(data) {
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
