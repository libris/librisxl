package whelk

import java.util.function.BiFunction
import java.util.function.Function
import groovy.util.logging.Log4j2 as Log

@Log
class Embellisher {
    static final List<String> EMBELLISH_LEVELS = ['cards', 'chips', 'chips']
    static final int MAX_REVERSE_LINKS = 512

    JsonLd jsonld
    Function<Iterable<String>, Iterable<Map>> getCards
    BiFunction<String, List<String>, Set<String>> getByReverseRelation

    Embellisher(
            JsonLd jsonld,
            Function<Iterable<String>, Iterable<Map>> getCards,
            BiFunction<String, List<String>, Set<String>> getByReverseRelation
    ) {
        this.jsonld = jsonld
        this.getCards = getCards
        this.getByReverseRelation = getByReverseRelation
    }

    // FIXME: describe me
    void embellish(Document document) {
        jsonld.embellish(document.data, getEmbellishData(document))
    }

    private List getEmbellishData(Document document) {
        if (document.getThingIdentifiers().isEmpty()) {
            return []
        }

        Iterable<Map> start = [document.data]

        Set<String> visitedIris = new HashSet<>()
        visitedIris.addAll(document.getThingIdentifiers())

        List<String> iris = getAllLinks(start)
        Iterable<Map> previousLevelDocs = start
        List embellishData = []
        for (String lens : EMBELLISH_LEVELS) {
            def cards = getCards.apply((List<String>) iris).collect()
            visitedIris.addAll(iris)
            visitedIris.addAll(cards.collectMany { new Document(it).getThingIdentifiers() })

            previousLevelDocs.each { insertInverseCards(lens, it, cards, visitedIris) }

            if (lens == 'chips') {
                cards = cards.collect{ (Map) jsonld.toChip(it) }
            }

            previousLevelDocs = cards
            embellishData.addAll(cards)

            iris = getAllLinks(cards)
            iris.removeAll(visitedIris)
        }
        // Last level: add reverse links, but not the documents linking here
        previousLevelDocs.each { insertInverseCards(EMBELLISH_LEVELS.last(), it, [], visitedIris) }

        return embellishData
    }

    private List<String> getAllLinks(Iterable<Map> docs) {
        (List<String>) docs.collect{ JsonLd.getExternalReferences((Map) it).collect{ it.iri } }.flatten()
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
            irisLinkingHere.removeAll(visitedIris)
            cards.addAll(getCards.apply(irisLinkingHere))
            visitedIris.addAll(irisLinkingHere)
        }
    }
}