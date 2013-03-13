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
        def links = collectIds(doc.dataAsJson, "", doc.identifier.toString())
        return links as Set
    }

    def collectIds(map, type, selfId) {
        def ids = []
        map.each { key, value ->
            if (key == "@id") {
                if (value != selfId) {
                    ids << new Link(new URI(value), type)
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
        return ids
    }
}
