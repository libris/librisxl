package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*
import se.kb.libris.whelks.exception.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class MarcCrackerAndLabelerIndexFormatConverter extends BasicPlugin implements IndexFormatConverter {

    String id = this.class.name
    boolean enabled = true
    ObjectMapper mapper
    def marcmap
    int order = 0

    def facit = [
        "020":   ["a":"isbn", "z":"isbn"],
        "022":   ["a":"issn", "m":"issn", "y":"issn", "z":"issn"],
        "100":   ["a":"author"],
        "505":   ["r":"author"],
        "700":   ["a":"author"],
        "243":   ["a":"title"],
        "245":   ["a":"title", "b": "title", "c": "author"],
        "246":   ["a":"title", "b": "title"],
        "247":   ["a":"title", "b": "title"],
        "720":   ["a":"title", "n": "title", "p": "title"]
    ]

    MarcCrackerAndLabelerIndexFormatConverter() {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream("marcmap.json")
        mapper = new ObjectMapper()
        this.marcmap = mapper.readValue(is, Map)
    }

    def expandField(fieldkey, ctrlfield, columns) {
        def l = [:]
        def propref
        int co = 0
        if (fieldkey == "006" || fieldkey == "007") {
            l['carrierType'] = ctrlfield[0]
        }
        for (def column : columns) {
            if (!column.propRef) {
                throw new WhelkRuntimeException("Propref is null for $ctrlfield and $columns")
            }
            if (propref != column.propRef) {
                l[column.propRef] = ""
            }
            try {
                if (column.length == 1) {
                    l[column.propRef] += ctrlfield[column.offset]
                } else {
                    l[column.propRef] += ctrlfield[(column.offset)..(column.offset+column.length-1)]
                }
            } catch (StringIndexOutOfBoundsException sioobe) {
                l.remove(column.propRef)
                break
            }
            propref = column.propRef
        }
        return l

    }

    @Override
    List<Document> convert(Document doc) {
        return convert([doc])
    }

    @Override
    List<Document> convert(List<Document> docs) {
        def outdocs = []
        for (doc in docs) {
            log.trace "Start convert on ${doc.dataAsString}"
            def json
            String d = doc.dataAsString
            try {
                /*
                if (d.contains("\\\"")) {
                d = d.replaceAll("\\\"", "/\"")
                }
                */
                json = mapper.readValue(doc.dataAsString, Map)

            } catch (Exception e) {
                log.error("Failed to parse document")
                log.error(doc.dataAsString, e)
                return null
            }
            def leader = json.leader 
            def pfx = doc.identifier.toString().split("/")[1]

            def l = expandField("000", leader, marcmap.get(pfx)."000".fixmaps[0].columns)

            json.leader = ["subfields": l.collect {key, value -> [(key):value]}]

            def mrtbl = l['typeOfRecord'] + l['bibLevel']
            log.trace "Leader extracted"

            json.fields.eachWithIndex() { it, pos ->
                log.trace "Working on json field $pos: $it"
                it.each { fkey, fvalue ->
                    if (fkey.startsWith("00")) {
                        if (fkey == "005") {
                            def date
                            try {
                                date = new Date().parse("yyyyMMddHHmmss.S", fvalue)
                            } catch (Exception e) {
                                date = new Date()
                            }
                            json.fields[pos] = [(fkey):date]
                        } else {
                            def matchKey = l['typeOfRecord']
                            if (fkey == "006" || fkey == "007") {
                                matchKey = fvalue[0]
                            }
                            marcmap.get(pfx).each { key, value ->
                                if (fkey == key) {
                                    try {
                                        value.fixmaps.each { fm ->
                                            if ((!fm.matchRecTypeBibLevel && fm.matchKeys.contains(matchKey)) || (fm.matchRecTypeBibLevel && fm.matchRecTypeBibLevel.contains(mrtbl))) {
                                                if (fkey == "008" && fvalue.length() == 39) {
                                                    log.warn("Document ${doc.identifier} has wrong length in 008")
                                                        fvalue = fvalue[0..19] + "|" + fvalue[20..-1]
                                                }
                                                json.fields[pos] = [(fkey):["subfields": expandField(fkey, fvalue, fm.columns).collect {k, v -> [(k):v] } ]]
                                            }
                                        }
                                    } catch (groovy.lang.MissingPropertyException mpe) { 
                                        log.warn("Exception in $fm : ${mpe.message}")
                                    } catch (Exception e) {
                                        log.error("Document identifier: ${doc.identifier}")
                                            log.error("fkey: $fkey")
                                            log.error("l: $l")
                                            throw e
                                    }

                                }
                            }
                        }
                    }
                }
            }

            json = appendLabels(json)


            try {
                outdocs << new BasicDocument(doc).withData(mapper.writeValueAsBytes(json))
            } catch (Exception e) {
                log.error("Failed to create cracked marc index: ${e.message}")
                log.error("JSON structure: $json")
                throw new se.kb.libris.whelks.exception.WhelkRuntimeException(e)
            }

        }
        return outdocs
    }

    def appendLabels(def json) {
        json.fields.each {
            it.each { field, data ->
                //log.debug("Field: $field : $data")
                if (facit.containsKey(field)) {
                    // log.debug("facit: " + facit[field])
                    facit[field].each { f, v ->
                        data["subfields"].each { pair ->
                            if (pair[f]) {
                                if (!json.labels) {
                                    json["labels"] = [:]
                                }
                                if (!json.labels[v]) {
                                    json.labels[v] = []
                                }
                                if (v == "isbn") {
                                    pair[f] = pair[f].replaceAll(/\D/, "")
                                }
                                json.labels[v].add(pair[f])
                                log.trace "Put "+ pair[f] + " in $v"
                            }
                        }
                    }
                }
            }
        }
        log.trace("Final json: $json")
        return json
    }


    void enable() { this.enabled = true }
    void disable() { this.enabled = false }
}
