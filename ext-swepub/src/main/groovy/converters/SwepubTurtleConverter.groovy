package whelk.swepub.plugin

import groovy.util.logging.Slf4j as Log

import whelk.*
import whelk.plugin.*

@Log
class SwepubTurtleConverter extends BasicRDFFormatConverter {

    String requiredContentType = "application/mods+xml"

    @Override
    Map<String, RDFDescription> doConvert(Document doc) {
        return null
    }
}
