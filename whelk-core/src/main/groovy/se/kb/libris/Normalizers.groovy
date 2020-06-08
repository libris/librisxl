package se.kb.libris

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.DocumentNormalizer
import whelk.filter.BlankNodeLinker
import whelk.filter.LanguageLinker

import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.ID_KEY

@Log
class Normalizers {

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

    /**
     * Historically locally defined Work was placed in @graph[2],
     * this normalizer makes sure it is always placed in mainEntity.instanceOf
     */
    static DocumentNormalizer workPosition(JsonLd jsonLd) {
        return { Document doc ->
            def (_record, thing, legacyWork) = doc.data[GRAPH_KEY]

            boolean shouldMove = (legacyWork && isInstanceOf(jsonLd, legacyWork, 'Work')
                    && thing && thing['instanceOf'] && thing['instanceOf'][ID_KEY]
                    && thing['instanceOf'][ID_KEY] == legacyWork[ID_KEY])

            if (shouldMove) {
                def work = doc.data[GRAPH_KEY].remove(2)
                work.remove(ID_KEY)
                thing['instanceOf'] = work
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
        def (_record, thing) = doc.data['@graph']
        if (thing && isInstanceOf(jsonLd, thing, 'Work')) {
            return thing
        }
        else if(thing && thing['instanceOf'] && isInstanceOf(jsonLd, thing['instanceOf'], 'Work')) {
            return thing['instanceOf']
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
