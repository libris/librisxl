package se.kb.libris.whelks.plugin

import groovy.util.logging.Slf4j as Log

import se.kb.libris.whelks.component.*

@Log
class UriToElasticType extends BasicPlugin implements ElasticShapeComputer {

    String defaultType
    String whelkName
    final static String URI_SEPARATOR = "::"

    UriToElasticType(Map settings) {
        this.defaultType = settings.get("defaultType", "record")
    }

    void bootstrap(String id) {
        this.whelkName = id
    }

    String calculateShape(String uri) {
        return calculateShape(new URI(uri))
    }

    String calculateShape(URI uri) {
        String identifier = uri.path.toString()
        log.debug("Received uri $identifier")
        String idxType
        try {
            def identParts = identifier.split("/")
            idxType = (identParts[1] == whelkName && identParts.size() > 3 ? identParts[2] : identParts[1])
        } catch (Exception e) {
            log.error("Tried to use first part of URI ${identifier} as type. Failed: ${e.message}", e)
        }
        if (!idxType) {
            idxType = defaultType
        }
        log.debug("Using type $idxType for ${identifier}")
        return idxType
    }

    String translateIdentifier(URI uri) {
        def idelements = uri.path.split("/") as List
        idelements.remove(0)
        return idelements.join(URI_SEPARATOR)
    }

    String translateIndexIdTo(id) {
        def pathelements = []
        id.split(URI_SEPARATOR).each {
            pathelements << java.net.URLEncoder.encode(it, "UTF-8")
        }
        return  new String("/"+pathelements.join("/"))
    }
}
