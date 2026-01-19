package whelk

import groovy.transform.CompileStatic
import groovy.transform.NullCheck

import whelk.component.DocumentNormalizer
import whelk.filter.BlankNodeLinker
import whelk.filter.LanguageLinker

import static whelk.JsonLd.ID_KEY as ID

// TODO: Make this a general interface for loading by type (or inScheme).
@CompileStatic
class ResourceCache {

    LanguageResources languageResources
    RelatorResources relatorResources

    private JsonLd jsonld
    private Whelk whelk
    private Map<String, Map<String, Map<String, Object>>> byTypeCache = [:]

    ResourceCache(JsonLd jsonld) {
      this.jsonld = jsonld
    }

    ResourceCache(Whelk whelk, LanguageLinker languageLinker, BlankNodeLinker relatorLinker) {
        this.whelk = whelk
        this.jsonld = whelk.jsonld
        // TODO: use getByType and no need for elasticFind!
        relatorResources = whelk.elasticFind ? new ResourceCache.RelatorResources(
                  relatorLinker: relatorLinker,
                  relators: whelk.elasticFind.find(['@type': ['Role']])
          ) : null
        languageResources = new ResourceCache.LanguageResources(
                languageLinker: languageLinker,
                languages: getByType('Language'),
                transformedLanguageForms: getByType('TransformedLanguageForm')
        )
    }

    // TODO: invalidate by time, or a new whelk.broadcastUpdate(id, type) signal...
    Map<String, Map<String, Object>> getByType(String type) {
        if (byTypeCache.containsKey(type)) {
          return byTypeCache[type]
        }

        Map<String, Map<String, Object>> descriptions = [:]

        whelk.loadAllByType(type).each {doc ->
            var thing = doc.getThing()
            String id = thing[ID]
            descriptions[id] = thing
        }
        whelk.jsonld.getSubClasses(type).forEach { subType ->
            whelk.loadAllByType(subType).each {doc ->
                Map<String, Object> thing = doc.getThing()
                String id = thing[ID]
                descriptions[id] = thing
            }
        }

        byTypeCache[type] = Collections.unmodifiableMap(descriptions)

        return descriptions
    }


    @CompileStatic
    //@NullCheck(includeGenerated = true)
    static class LanguageResources {
        LanguageLinker languageLinker
        Map<String, Map<String, Object>> languages
        Map<String, Map<String, Object>> transformedLanguageForms
    }

    @CompileStatic
    static class RelatorResources {
        BlankNodeLinker relatorLinker
        Iterable<Map> relators
    }
}
