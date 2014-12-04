package whelk.plugin

import whelk.ElasticQuery
import whelk.Document
import whelk.Whelk

import static whelk.util.Tools.*

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

