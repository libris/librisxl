package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDEntityExtractorIndexFormatConverter extends BasicIndexFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def authPoint = ["Person": "controlledLabel/label", "Concept": "prefLabel", "ConceptScheme": "notation"]
    def entitiesToExtract = ["about.inScheme", "about.instanceOf.creator", "about.instanceOf.contributorList"]

    List<IndexDocument> doConvert(Document doc) {
        log.debug("Converting indexdoc $doc.identifier")

        List<IndexDocument> doclist = [new IndexDocument(doc)]

        def json = mapper.readValue(doc.dataAsString, Map)

        if (json) {

            if (json.about?.get("@type", null)) {
                if (authPoint.containsKey(json.about.get("@type"))) {
                    log.debug("Extracting authority entity " + json.about.get("@type"))
                    doclist << createEntityDoc(json.about, doc.identifier, 10, false)
                }
            }

            for (it in entitiesToExtract) {
                def jsonMap = json
                def propList = it.tokenize(".")
                for (prop in propList) {
                    try {
                        jsonMap = jsonMap[prop]
                    } catch (Exception e) {
                        jsonMap = null
                        break
                    }
                }
                if (jsonMap) {
                    log.debug("Extracting entity $it")
                    doclist += extractEntities(jsonMap, doc.identifier, 1)
                }
            }
        }
        return doclist
    }

    List<IndexDocument> extractEntities(def extractedJson, def id, def prio) {
        List<IndexDocument> entityDocList = []
        if (extractedJson instanceof List) {
            for (entity in extractedJson) {
                def entityDoc = createEntityDoc(entity, id, prio, true)
                if (entityDoc) {
                    entityDocList << entityDoc
                }
            }
        } else if (extractedJson instanceof Map) {
            def entityDoc = createEntityDoc(extractedJson, id, prio, true)
            if (entityDoc) {
                entityDocList << entityDoc
            }
        }
        return entityDocList
    }

    IndexDocument createEntityDoc(def entityJson, def docId, def prio, def slugifyId) {
        try {
            def type = entityJson["@type"]
            String pident = docId
            if (slugifyId) {
                def authPath
                def label = authPoint.get(type, null)
                if (!label) {
                    log.debug("Type $type not declared for index entity extraction.")
                    return null
                }
                for (l in label.tokenize("/")) {
                    if (!authPath) {
                        authPath = entityJson.get(l, null)
                    }
                }
                pident = slugify(authPath, docId, type)
                entityJson["extractedFrom"] = ["@id": docId]
            } else {
                entityJson["@id"] = pident
            }
            entityJson["recordPriority"] = prio
            entityJson.get("unknown", null) ?: entityJson.remove("unknown")
            log.debug("Created indexdoc $pident with prio $prio")
            return new IndexDocument().withData(mapper.writeValueAsBytes(entityJson)).withContentType("application/ld+json").withIdentifier(pident).withType(type)
        } catch (Exception e) {
            log.debug("Could not create entitydoc ${e} docId: $docId" + " EntityJson " + mapper.writeValueAsString(entityJson))
            return null
        }
    }

    String slugify(String authAccPoint, URI identifier, String entityType) {
        String uritype = identifier.path.split("/")[1]
        return new String("/" + uritype + "/" + entityType + "/" + URLEncoder.encode(authAccPoint))
    }
}
