package whelk

import java.util.function.BiFunction
import java.util.function.Function
import groovy.util.logging.Log4j2 as Log

@Log
class Embellisher {
    static final List<String> DEFAULT_EMBELLISH_LEVELS = ['cards', 'chips']
    // FIXME: get from context
    static final List<String> DEFAULT_CLOSE_RELATIONS = ['instanceOf', 'translationOf']

    static final int MAX_REVERSE_LINKS = 512

    JsonLd jsonld
    List<String> embellishLevels = DEFAULT_EMBELLISH_LEVELS
    List<String> closeRelations = DEFAULT_CLOSE_RELATIONS

    Function<Iterable<String>, Iterable<Map>> getDocs
    Function<Iterable<String>, Iterable<Map>> getCards
    BiFunction<String, List<String>, Set<String>> getByReverseRelation

    Embellisher(
            JsonLd jsonld,
            Function<Iterable<String>, Iterable<Map>> getDocs,
            Function<Iterable<String>, Iterable<Map>> getCards,
            BiFunction<String, List<String>, Set<String>> getByReverseRelation
    ) {
        this.jsonld = jsonld
        this.getDocs = getDocs
        this.getCards = getCards
        this.getByReverseRelation = getByReverseRelation
    }

    // FIXME: describe me
    void embellish(Document document) {
        jsonld.embellish(document.data, getEmbellishData(document))
    }

    void setEmbellishLevels(List<String> embellishLevels) {
        this.embellishLevels = embellishLevels.collect()
    }

    void setCloseRelations(List<String> closeRelations) {
        this.closeRelations = closeRelations.collect()
    }

    private List getEmbellishData(Document document) {
        if (document.getThingIdentifiers().isEmpty()) {
            return []
        }

        List<Map> start = [document.data]

        Set<String> visitedIris = new HashSet<>()
        visitedIris.addAll(plusWithoutHash(document.getThingIdentifiers()))

        def docs = fetchClose('full', start, visitedIris).collect()

        List result = docs
        List<String> iris = getAllLinks(start + docs)
        Iterable<Map> previousLevelDocs = start + docs
        String previousLens = 'full'

        for (String lens : embellishLevels) {
            docs = fetchNonVisited(lens, iris, visitedIris)
            docs += fetchClose(lens, docs, visitedIris)

            previousLevelDocs.each { insertInverse(previousLens, it, lens, docs, visitedIris) }
            previousLevelDocs = docs
            previousLens = lens

            result.addAll(docs)

            iris = getAllLinks(docs)
        }
        // Last level: add reverse links, but don't include documents linking here in embellish graph
        previousLevelDocs.each { insertInverse(previousLens, it, null, [], visitedIris) }

        return result
    }

    private static List<String> getAllLinks(Iterable<Map> docs) {
        (List<String>) docs.collect{ JsonLd.getExternalReferences((Map) it).collect{ it.iri } }.flatten()
    }

    private List<String> getCloseLinks(Iterable<Map> docs) {
        (List<String>) docs.collect{
            JsonLd.getExternalReferences((Map) it).grep{ it.relation in closeRelations }.collect{ it.iri }
        }.flatten()
    }

    private Iterable<Map> fetchNonVisited(String lens, Iterable<String> iris, Set<String> visitedIris) {
        def data = load(lens, iris - visitedIris)
        visitedIris.addAll(data.collectMany { plusWithoutHash(new Document(it).getThingIdentifiers()) })
        visitedIris.addAll(iris)
        return data
    }

    private Iterable<Map> fetchClose(String lens, Iterable<Map> docs, Set<String> visitedIris) {
        def result = []
        while(true) {
            docs = fetchNonVisited(lens, getCloseLinks(docs), visitedIris)

            if (docs.isEmpty()) {
                break
            }
            else {
                result.addAll(docs)
            }
        }

        return result
    }

    private Iterable<Map> load(String lens, Iterable<String> iris) {
        if (iris.isEmpty()) {
            return []
        }

        def data = lens == 'full'
                ? getDocs.apply(iris)
                : getCards.apply(iris)

        if (lens == 'chips') {
            data = data.collect{ (Map) jsonld.toChip(it) }
        }

        return data
    }

    private void insertInverse(String forLens, Map thing, String applyLens, List<Map> cards, Set<String> visitedIris) {
        Set<String> inverseRelations = jsonld.getInverseProperties(thing, forLens)
        if (inverseRelations.isEmpty()) {
            return
        }

        String iri = new Document(thing).getThingIdentifiers().first()
        for (String relation : inverseRelations) {
            Set<String> irisLinkingHere = getByReverseRelation.apply(iri, [relation])
            if (irisLinkingHere.isEmpty()) {
                continue
            }

            if (irisLinkingHere.size() > MAX_REVERSE_LINKS) {
                log.warn("MAX_REVERSE_LINKS exceeded. $iri $JsonLd.REVERSE_KEY $relation " +
                        "(${irisLinkingHere.size()} > $MAX_REVERSE_LINKS)")
                irisLinkingHere = irisLinkingHere.take(MAX_REVERSE_LINKS).toSet()
            }

            Map theThing = ((List) thing[JsonLd.GRAPH_KEY])[1]
            if (!theThing[JsonLd.REVERSE_KEY]) {
                theThing[JsonLd.REVERSE_KEY] = [:]
            }

            theThing[JsonLd.REVERSE_KEY][relation] = irisLinkingHere.collect { [(JsonLd.ID_KEY): it] }
            if (applyLens) {
                cards.addAll(fetchNonVisited(applyLens, irisLinkingHere, visitedIris))
            }
        }
    }

    private static List<String> plusWithoutHash(List<String> iris) {
        return iris + iris.findResults { if(it.contains('#')) {it.substring(0, it.indexOf('#'))} }
    }
}