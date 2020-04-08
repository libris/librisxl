package whelk

import java.util.function.BiFunction
import java.util.function.Function
import groovy.util.logging.Log4j2 as Log

@Log
class Embellisher {
    static final List<String> DEFAULT_EMBELLISH_LEVELS = ['cards', 'chips']
    // FIXME: get from context
    static final List<String> DEFAULT_CLOSE_RELATIONS = ['instanceOf']

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
        visitedIris.addAll(document.getThingIdentifiers())

        def docs = fetchNonVisited('full', getCloseLinks(start), visitedIris).collect()

        List result = docs
        List<String> iris = getAllLinks(start + docs)
        Iterable<Map> previousLevelDocs = start + docs

        for (String lens : embellishLevels) {
            docs = fetchNonVisited(lens, iris, visitedIris)
            docs += fetchNonVisited(lens, getCloseLinks(docs), visitedIris)

            previousLevelDocs.each { insertInverseCards(lens, it, docs, visitedIris) }
            previousLevelDocs = docs

            result.addAll(docs)

            iris = getAllLinks(docs)
        }
        // Last level: add reverse links, but not the documents linking here
        previousLevelDocs.each { insertInverseCards(embellishLevels.last(), it, [], visitedIris) }

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
        visitedIris.addAll(data.collectMany { new Document(it).getThingIdentifiers() })
        return data
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

    private void insertInverseCards(String lens, Map thing, List<Map> cards, Set<String> visitedIris) {
        Set<String> inverseRelations = jsonld.getInverseProperties(thing, lens)
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
            cards.addAll(fetchNonVisited(lens, irisLinkingHere, visitedIris))
        }
    }
}