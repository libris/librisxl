package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.component.ElasticQuery
import se.kb.libris.whelks.Document
import se.kb.libris.whelks.Whelk

import static se.kb.libris.conch.Tools.*

import org.codehaus.jackson.map.ObjectMapper
import groovy.util.logging.Slf4j as Log

@Log
class JsonLDSerialsLinkFinderFilter extends BasicFilter implements WhelkAware {
    Whelk whelk
    String requiredContentType = "application/ld+json"

    @Override
    Document doFilter(Document doc) {
        return doc
    }

    @Override
    boolean valid(Document doc) {
        return false
    }
}

