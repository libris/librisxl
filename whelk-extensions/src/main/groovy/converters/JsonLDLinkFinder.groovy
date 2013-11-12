package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log

import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDLinkFinder extends BasicPlugin implements LinkFinder {

    ObjectMapper mapper

    @Override
    Set<Link> findLinks(Document doc) {
        def links = []
        if (doc && (doc.contentType == "application/json" || doc.contentType == "application/ld+json")) {
            links = collectIds(doc.dataAsMap, "", doc.identifier)
        }
        return links as Set
    }

    def collectIds(map, type, selfId) {
        def ids = []
        if (map instanceof Map) {
            map.each { key, value ->
                if (key == "@id") {
                    if (value instanceof String && value != selfId && !value.startsWith("_:")) {
                        value.split().each {
                            ids << new Link(new URI(it), type)
                        }
                    }
                }
                if (value instanceof Map) {
                    ids.addAll(collectIds(value, key, selfId))
                }
                if (value instanceof List) {
                    value.each {
                        ids.addAll(collectIds(it, key, selfId))
                    }
                }
            }
        }
        return ids
    }
}
