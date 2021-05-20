package se.kb.libris

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.DocumentNormalizer
import whelk.exception.InvalidQueryException
import whelk.filter.BlankNodeLinker
import whelk.filter.LanguageLinker

import static whelk.JsonLd.GRAPH_KEY
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
    
    static Collection<DocumentNormalizer> heuristicLinkers(Whelk whelk) {
        whelk.jsonld.getCategoryMembers('heuristicIdentity').collect{ type ->
            BlankNodeLinker linker = new BlankNodeLinker(
                    type, ['code', 'label', 'prefLabelByLang', 'altLabelByLang', 'hiddenLabel'])
            loadDefinitions(linker, whelk)

            Set<String> inRange = whelk.jsonld.getInRange(type)
            return (DocumentNormalizer) { doc ->
                linker.linkAll(doc.data, inRange)
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

    static void enforceTypeSingularity(node, jsonLd) {
        if (node instanceof Map) {
            for (Object key: node.keySet()) {

                if (key.equals("@type")) {
                    Object typeObject = node[key]
                    if (typeObject instanceof List) {
                        List typeList = typeObject

                        List typesToRemove = []
                        for (Object type : typeList) {
                            jsonLd.getSuperClasses(type, typesToRemove)
                        }
                        typeList.removeAll(typesToRemove)

                        if (typeList.size() == 1)
                            node[key] = typeList[0]
                        else {

                            //throw new ModelValidationException("Could not reduce: " + typeList + " to a single type (required) by removing superclasses.")
                            // This must be reduced to a warning, because the assumption does not hold true: There are records in
                            // libris that legitimately have unrelated multi-types (see definitions).
                            log.warn("Could not reduce: " + typeList + " to a single type by removing superclasses.")
                        }
                    }
                }

                else {
                    enforceTypeSingularity(node[key], jsonLd)
                }
            }
        } else if (node instanceof List) {
            for (Object element : node) {
                enforceTypeSingularity(element, jsonLd)
            }
        }
    }

    static DocumentNormalizer typeSingularity(JsonLd jsonLd) {
        return { Document doc ->
            enforceTypeSingularity(doc.data, jsonLd)
        }
    }

    static void loadDefinitions(BlankNodeLinker linker, Whelk whelk) {
        try {
            linker.loadDefinitions(whelk)
            log.info("Loaded normalizer: $linker")
        }
        catch (InvalidQueryException e) {
            log.warn("Failed to load definitions for $linker: $e. Newly created, empty ES index?")
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
