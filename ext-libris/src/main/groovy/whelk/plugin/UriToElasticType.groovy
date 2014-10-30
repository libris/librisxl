package whelk.plugin

import groovy.util.logging.Slf4j as Log

import org.apache.commons.codec.binary.Base64

import whelk.component.*

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

    String calculateTypeFromIdentifier(String id) {
        String identifier = new URI(id).path.toString()
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

    String toElasticId(String id) {
        return Base64.encodeBase64URLSafeString(id.getBytes("UTF-8"))
    }

    String fromElasticId(String id) {
        if (id.contains(URI_SEPARATOR)) {
            log.warn("Using old style index id's for $id")
            def pathelements = []
            id.split(URI_SEPARATOR).each {
                pathelements << java.net.URLEncoder.encode(it, "UTF-8")
            }
            return  new String("/"+pathelements.join("/"))
        } else {
            String decodedIdentifier = new String(Base64.decodeBase64(id), "UTF-8")
            log.debug("Decoded new style id into $decodedIdentifier")
            return decodedIdentifier
        }
    }
}
