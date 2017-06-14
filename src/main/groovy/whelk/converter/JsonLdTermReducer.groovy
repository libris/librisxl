package whelk.converter

import groovy.util.logging.Log4j2 as Log

import whelk.Document

@Log
class JsonLdTermReducer {

    boolean valid(Document doc) {
        return doc && doc.isJson() && doc.contentType == "application/ld+json"
    }

    Document doFilter(Document doc) {
        def dataMap = doFilter(doc.dataAsMap, doc.dataset)
        return doc.withData(dataMap)
    }

    Map doFilter(Map dataMap, String dataset) {
        def thing = dataMap.about ?: dataMap.focus ?: dataMap
        reduce(thing)
        return dataMap
    }

    void reduce(thing) {

        add thing, 'title', thing.instanceTitle?.titleValue

        add thing, 'isbn', thing.identifier?.findAll {
            it.identifierScheme?.get("@id") == "/def/identifiers/isbn"
        }.collect {
            it.identifierValue?.replaceAll("-", "")
        }

    }

    void add(owner, term, value) {
        // TODO: treat result as a set; i.e. don't add if value or ID is present
        if (value) {
            def container = owner[term]
            if (container instanceof List) {
                container += value
            } else {
                owner[term] = value
            }
        }
    }

    List asList(value) {
        return value instanceof List? value : [value]
    }
}
