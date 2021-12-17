package whelk

import java.util.function.BiFunction
import java.util.function.Function
import groovy.util.logging.Log4j2 as Log

@Log
class Embellisher {
    static final List<String> DEFAULT_EMBELLISH_LEVELS = ['cards', 'chips']
    static final List<String> DEFAULT_INTEGRAL_RELATIONS = ['instanceOf', 'translationOf']

    static final int MAX_REVERSE_LINKS = 512

    JsonLd jsonld
    Collection<String> embellishLevels = DEFAULT_EMBELLISH_LEVELS
    Collection<String> integralRelations = DEFAULT_INTEGRAL_RELATIONS

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
        
        def integral = jsonld.getCategoryMembers('integral')
        if (integral) {
            this.integralRelations = integral
        }
    }

    // FIXME: describe me
    void embellish(Document document) {
        jsonld.embellish(document.data, getEmbellishData(document))
    }

    void setEmbellishLevels(List<String> embellishLevels) {
        this.embellishLevels = embellishLevels.collect()
    }

    void setIntegralRelations(List<String> integralRelations) {
        this.integralRelations = integralRelations.collect()
    }

    private List getEmbellishData(Document document) {
        if (document.getThingIdentifiers().isEmpty()) {
            return []
        }

        List<Map> start = [document.data]

        Set<String> visitedIris = new HashSet<>()
        visitedIris.addAll(plusWithoutHash(document.getThingIdentifiers()))

        def docs = fetchIntegral('full', start, integral(getAllLinks(start)), visitedIris).collect()

        List result = docs
        Set<Link> links = getAllLinks(start + docs)
        Iterable<Map> previousLevelDocs = start + docs
        String previousLens = 'full'

        for (String lens : embellishLevels) {
            docs = fetchNonVisited(lens, uniqueIris(links), visitedIris)
            links = getAllLinks(docs)
            
            def integralDocs = fetchIntegral(lens, docs, integral(links), visitedIris)
            docs += integralDocs
            links += getAllLinks(integralDocs)
            
            previousLevelDocs.each {
                def inverseDocs = insertInverse(previousLens, it, lens, visitedIris)
                docs += inverseDocs
                links += getAllLinks(inverseDocs)
            }
            previousLevelDocs = docs
            previousLens = lens

            result += docs
        }
        // Last level: add reverse links, but don't include documents linking here in embellish graph
        previousLevelDocs.each { insertInverse(previousLens, it, null, visitedIris) }

        return result
    }
    
    private static Set<Link> getAllLinks(Iterable<Map> docs) {
        Set<Link> links = new HashSet<>()
        for (Map doc : docs) { 
            links += JsonLd.getExternalReferences(doc) 
        }
        return links
    }

    private static Set<String> uniqueIris(Set<Link> links) {
        Set<String> iris = new HashSet<>(links.size())
        for (Link link : links) { 
            iris.add(link.iri) 
        }
        return iris
    }

    private Set<Link> integral(Set<Link> links) {
        Set<Link> integral = new HashSet<>()
        for (Link l : links) { 
            if (l.relation in integralRelations) { 
                integral.add(l) 
            } 
        }
        return integral
    }

    private Iterable<Map> fetchNonVisited(String lens, Iterable<String> iris, Set<String> visitedIris) {
        def data = load(lens, iris - visitedIris)
        visitedIris.addAll(data.collectMany { plusWithoutHash(new Document(it).getThingIdentifiers()) })
        visitedIris.addAll(iris)
        return data
    }

    private Iterable<Map> fetchIntegral(String lens, Iterable<Map> docs, Set<Link> integralLinks, Set<String> visitedIris) {
        def result = []
        while(true) {
            docs = fetchNonVisited(lens, uniqueIris(integralLinks), visitedIris)
            integralLinks = integral(getAllLinks(docs))

            if (docs.isEmpty()) {
                break
            }
            else {
                result += docs
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

    private Iterable<Map> insertInverse(String forLens, Map thing, String applyLens, Set<String> visitedIris) {
        Set<String> inverseRelations = jsonld.getInverseProperties(thing, forLens)
        if (inverseRelations.isEmpty()) {
            return Collections.EMPTY_LIST
        }

        List<Map> cards = []
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
        return cards
    }

    private static List<String> plusWithoutHash(List<String> iris) {
        return iris + iris.findResults { if(it.contains('#')) {it.substring(0, it.indexOf('#'))} }
    }
}