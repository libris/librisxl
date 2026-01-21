package whelk

import groovy.transform.CompileStatic
import groovy.transform.NullCheck

import whelk.component.DocumentNormalizer
import whelk.filter.BlankNodeLinker
import whelk.filter.LanguageLinker

import static whelk.JsonLd.asList
import static whelk.JsonLd.ID_KEY as ID
import static whelk.JsonLd.TYPE_KEY as TYPE
import static whelk.JsonLd.REVERSE_KEY as REVERSE

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

    // TODO: invalidate by time, or a new whelk.broadcastUpdate(id, type) signal... (there is one! See PostgreSQLComponent?)
    Map<String, Map<String, Object>> getByType(String type) {
        if (byTypeCache.containsKey(type)) {
          return byTypeCache[type]
        }

        Map<String, Map<String, Object>> descriptions = [:]

        if (!whelk) {
          return descriptions
        }

        // NOTE: Since whelk.loadAllByType does not index/find resources having
        // multiple types, we need to either fallback to elastic, or (as done
        // now), load a specific hardcoded dataset which happens to contain the
        // only ones we use multiple types on (i.e. the MARC enum types).
        // TODO: Remove this hack if we fix loadAllByType!
        if (type.startsWith('marc:')) {
          var enumsDataset = 'https://id.kb.se/dataset/enums' // TODO: configure? Or live with...
          whelk.storage.loadAll(enumsDataset).each { doc ->
              Map<String, Object> thing = doc.getThing()
              if (asList(thing[TYPE]).any { it instanceof String && jsonld.isSubClassOf(it, type) }) {
                String id = thing[ID]
                descriptions[id] = thing
              }
          }
        }

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

        descriptions = Collections.unmodifiableMap(descriptions)
        byTypeCache[type] = descriptions

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
