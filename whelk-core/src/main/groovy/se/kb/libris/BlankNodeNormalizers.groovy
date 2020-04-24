package se.kb.libris

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.DocumentNormalizer
import whelk.filter.BlankNodeLinker
import whelk.filter.LanguageLinker

@Log
class BlankNodeNormalizers {

    static DocumentNormalizer language(Whelk whelk) {
        LanguageLinker linker = new LanguageLinker()
        loadDefinitions(linker, whelk)

        return { Document doc ->
            linker.linkAll(doc.data, 'associatedLanguage')
            linker.linkAll(doc.data, 'language')
        }
    }

    static DocumentNormalizer contributionRole(Whelk whelk) {
        BlankNodeLinker linker = new BlankNodeLinker(
                'Role', ['code', 'label', 'prefLabelByLang', 'altLabelByLang', 'hiddenLabel'])
        loadDefinitions(linker, whelk)

        return { Document doc ->
            Map work = getWork(whelk.jsonld, doc)
            if (work && work['contribution']) {
                linker.linkAll(work['contribution'], 'role')
            }
        }
    }

    static void loadDefinitions(BlankNodeLinker linker, Whelk whelk) {
        try {
            linker.loadDefinitions(whelk)
            log.info("Loaded normalizer: $linker")
        }
        catch (Exception e) {
            log.warn("Failed to load definitions for $linker: $e", e)
        }
    }

    static Map getWork(JsonLd jsonLd, Document doc) {
        def (record, thing, legacyWork) = doc.data['@graph']
        if (thing && isInstanceOf(jsonLd, thing, 'Work')) {
            return thing
        }
        else if(thing && thing['instanceOf'] && isInstanceOf(jsonLd, thing['instanceOf'], 'Work')) {
            return thing['instanceOf']
        }
        else if (legacyWork && isInstanceOf(jsonLd, legacyWork, 'Work')) {
            return legacyWork
        }
        return null
    }

    static boolean isInstanceOf(JsonLd jsonLd, Map entity, String baseType) {
        def type = entity['@type']
        if (type == null)
            return false
        def types = type instanceof String ? [type] : type
        return types.any { jsonLd.isSubClassOf(it, baseType) }
    }
}
