package se.kb.libris

import groovy.util.logging.Log4j2 as Log
import whelk.Document
import whelk.JsonLd
import whelk.Whelk
import whelk.component.DocumentNormalizer
import whelk.exception.InvalidQueryException
import whelk.filter.BlankNodeLinker
import whelk.filter.LanguageLinker
import whelk.util.DocumentUtil
import whelk.util.DocumentUtil.Remove
import whelk.util.Romanizer

import static whelk.JsonLd.GRAPH_KEY
import static whelk.JsonLd.ID_KEY
import static whelk.JsonLd.TYPE_KEY
import static whelk.JsonLd.asList
import static whelk.util.DocumentUtil.traverse

/*
TODO: add support for linking blank nodes based on owl:hasKey
example: 
:Concept a owl:Class;
    rdfs:label "Concept"@en, "Koncept"@sv;
    rdfs:subClassOf :Identity;
    owl:equivalentClass skos:Concept;
    owl:hasKey (:code :prefLabel :inScheme) .

(only hasKey defined in vocab at the moment)
 */

@Log
class Normalizers {
    static DocumentNormalizer nullRemover() {
        return new Normalizer({ Document doc ->
            traverse(doc.data, { value, path ->
                if (value == null) {
                    new Remove()
                }
            })
        })
    }

    static DocumentNormalizer language(LanguageLinker linker) {
        return new Normalizer(linker, { Document doc, LanguageLinker l = linker ->
            l.linkAll(doc.data, 'associatedLanguage')
            l.linkAll(doc.data, 'language')
        })
    }

    /**
     * Link blank nodes based on "heuristic identifiers"
     * e.g. { "@type": "Role", "label": "Þýðandi"} matches https://id.kb.se/relator/trl on prefLabelByLang.is
     *
     * For all types that have :category :heuristicIdentity in vocab:
     * Link all blank nodes with that @type that match on a property that has :category :heuristicIdentifier.
     * Only check blank nodes in properties where @type is in range (range or rangeIncludes).
     */
    static Collection<DocumentNormalizer> heuristicLinkers(Whelk whelk, Collection<String> skipTypes) {
        def properties = whelk.jsonld.getCategoryMembers('heuristicIdentifier').collect()
        properties = properties + properties.findResults { (String) whelk.jsonld.langContainerAlias[it] }

        whelk.jsonld.getCategoryMembers('heuristicIdentity').minus(skipTypes).collect { type ->
            BlankNodeLinker linker = new BlankNodeLinker(type, properties)
            loadDefinitions(linker, whelk)

            Set<String> inRange = whelk.jsonld.getInRange(type)

            return new Normalizer(linker, { Document doc, BlankNodeLinker l = linker ->
                l.linkAll(doc.data, inRange)
            })
        }
    }

    static DocumentNormalizer identifiedBy() {
        def OBSOLETE_TYPE_NOTES = [
                'ansi'    : 'Ansi',
                'doi'     : 'DOI',
                'danacode': 'Danacode',
                'gtin-14' : 'GTIN14',
                'hdl'     : 'Hdl',
                'isan'    : 'ISAN',
                'isni'    : 'ISNI',
                'iso'     : 'ISO',
                'istc'    : 'ISTC',
                'iswc'    : 'ISWC',
                'orcid'   : 'ORCID',
                'uri'     : 'URI',
                'urn'     : 'URN',
                'viaf'    : 'VIAF',
                'wikidata': 'WikidataID',
                // already typed in Marc bib 028 ind1:
                //if $2=matrix-number, then I - identifiedBy - MatrixNumber;
                //if $2=music-plate, then I - identifiedBy - MusicPlate;
                //if $2=music-publisher, then I - identifiedBy - MusicPublisherNumber;
                //if $2=videorecording-identifer, then I - identifiedBy - VideoRecordingNumber;
        ]

        return new Normalizer({ Document doc ->
            def (_record, thing) = doc.data[GRAPH_KEY]
            thing.identifiedBy?.with {
                asList(it).forEach { Map id ->
                    id.typeNote?.with { String tn ->
                        OBSOLETE_TYPE_NOTES[tn.toLowerCase()]
                    }?.with { type ->
                        id[TYPE_KEY] = type
                        id.remove('typeNote')
                    }
                }
                asList(it).findAll { Map id -> Document.isIsni(id) || Document.isOrcid(id) }.forEach { Map isni ->
                    if (isni.containsKey('value')) {
                        isni.value = ((String) isni.value).replace(' ', '')
                    }
                }
            }
        })
    }

    /**
     * Historically locally defined Work was placed in @graph[2],
     * this normalizer makes sure it is always placed in mainEntity.instanceOf
     */
    static DocumentNormalizer workPosition(JsonLd jsonLd) {
        return new Normalizer({ Document doc ->
            def (_record, thing, legacyWork) = doc.data[GRAPH_KEY]

            boolean shouldMove = (legacyWork && isInstanceOf(jsonLd, legacyWork, 'Work')
                    && thing && thing['instanceOf'] && thing['instanceOf'][ID_KEY]
                    && thing['instanceOf'][ID_KEY] == legacyWork[ID_KEY])

            if (shouldMove) {
                def work = doc.data[GRAPH_KEY].remove(2)
                work.remove(ID_KEY)
                thing['instanceOf'] = work
            }
        })
    }

    static void enforceTypeSingularity(node, jsonLd) {
        if (node instanceof Map) {
            for (Object key : node.keySet()) {

                if (key == "@type") {
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
                } else {
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
        return new Normalizer({ Document doc ->
            enforceTypeSingularity(doc.data, jsonLd)
        })
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
        } else if (thing && thing['instanceOf'] && isInstanceOf(jsonLd, thing['instanceOf'], 'Work')) {
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

class Normalizer implements DocumentNormalizer {
    Object normalizer
    Closure normalizeFunc

    Normalizer(Object normalizer, Closure normalizeFunc) {
        this.normalizer = normalizer
        this.normalizeFunc = normalizeFunc
    }

    Normalizer(Closure normalizeFunc) {
        this.normalizeFunc = normalizeFunc
    }

    void normalize(Document doc) {
        normalizeFunc(doc)
    }
}
