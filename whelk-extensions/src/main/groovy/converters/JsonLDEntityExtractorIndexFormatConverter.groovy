package se.kb.libris.whelks.plugin

import se.kb.libris.whelks.*
import se.kb.libris.whelks.basic.*

import groovy.util.logging.Slf4j as Log
import org.codehaus.jackson.map.ObjectMapper

@Log
class JsonLDEntityExtractorIndexFormatConverter extends BasicIndexFormatConverter {

    String requiredContentType = "application/ld+json"
    ObjectMapper mapper = new ObjectMapper()
    def authPoint = ["Person": "controlledLabel", "Concept": "prefLabel", "ConceptScheme": "notation", "Organization" : "name"]
    def entitiesToExtract = ["about.inScheme", "about.instanceOf.attributedTo", "about.instanceOf.influencedBy"]

    List<IndexDocument> doConvert(Document doc) {
        log.debug("Converting indexdoc $doc.identifier")

        def docType = new URI(doc.identifier).path.split("/")[1]

        List<IndexDocument> doclist = [new IndexDocument(doc)]

        def json = doc.dataAsMap

        if (json) {

            if (json.about?.get("@type")) {
                if (authPoint.containsKey(json.about.get("@type"))) {
                    log.debug("Extracting authority entity " + json.about.get("@type"))
                    /*def slugId = false
                    if (json.about.get("@type").equals("ConceptScheme")) {
                        slugId = true
                    } */
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
                    doclist += extractEntities(jsonMap, doc.identifier, docType, 1)
                }
            }
        }
        return doclist
    }

    List<IndexDocument> extractEntities(extractedJson, id, String type, prio) {
        List<IndexDocument> entityDocList = []
        if (extractedJson instanceof List) {
            for (entity in extractedJson) {
                if (!(type.equals("bib") && entity.get("@id"))) {  //only extract bib-entity that doesn't link to existing authority
                    def entityDoc = createEntityDoc(entity, id, prio, true)
                    if (entityDoc) {
                        entityDocList << entityDoc
                    }
                }
            }
        } else if (extractedJson instanceof Map) {
            if (!(type.equals("bib") && extractedJson.get("@id"))) {  //only extract bib-entity that doesn't link to existing authority
                def entityDoc = createEntityDoc(extractedJson, id, prio, true)
                if (entityDoc) {
                    entityDocList << entityDoc
                }
            }
        }
        return entityDocList
    }

    IndexDocument createEntityDoc(def entityJson, def docId, def prio, def slugifyId) {
        try {
            def indexId = entityJson["@id"]
            def type = entityJson["@type"]
            if (slugifyId) {
                def label = authPoint.get(type, null)
                def authPath = entityJson[label]
                if (!label) {
                    log.debug("Type $type not declared for index entity extraction.")
                    return null
                }
                indexId = slugify(authPath, new URI(docId), type)
            }
            entityJson["extractedFrom"] = ["@id": docId]
            entityJson["recordPriority"] = prio
            entityJson.get("unknown", null) ?: entityJson.remove("unknown")
            log.debug("Created indexdoc ${indexId} with prio $prio")
            return new IndexDocument().withData(mapper.writeValueAsBytes(entityJson)).withContentType("application/ld+json").withIdentifier(indexId).withType(type)
        } catch (Exception e) {
            log.debug("Could not create entitydoc ${e} from docId: $docId" + " EntityJson " + mapper.writeValueAsString(entityJson))
            return null
        }
    }

    String slugify(String authAccPoint, URI identifier, String entityType) {
        String uritype = identifier.path.split("/")[1]
        return new String("/" + uritype + "/" + entityType + "/" + URLEncoder.encode(authAccPoint))
    }
}
