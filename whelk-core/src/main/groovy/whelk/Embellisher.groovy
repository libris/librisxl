package whelk

import groovy.transform.CompileStatic

import java.util.function.BiFunction
import java.util.function.Function
import groovy.util.logging.Log4j2 as Log

@Log
@CompileStatic
class Embellisher {
    static final List<String> DEFAULT_EMBELLISH_LEVELS = ['cards', 'chips']
    static final List<String> DEFAULT_INTEGRAL_RELATIONS = ['instanceOf', 'translationOf']

    static final int MAX_REVERSE_LINKS = 512

    JsonLd jsonld
    Collection<String> embellishLevels = DEFAULT_EMBELLISH_LEVELS
    Collection<String> integralRelations = DEFAULT_INTEGRAL_RELATIONS
    Collection<String> inverseIntegralRelations = []
    boolean followInverse = true

    Function<Iterable<String>, Iterable<Map>> getDocs
    Function<Iterable<String>, Iterable<Map>> getCards
    BiFunction<String, List<String>, Set<String>> getByReverseRelation

    Function<String, Set<String>> _getAllBroaderIds

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
        
        def integral = jsonld.getCategoryMembers(JsonLd.Category.INTEGRAL)
        if (integral) {
            this.integralRelations = integral
            this.inverseIntegralRelations = integralRelations.collect{ jsonld.getInverseProperty(it) }.grep()
        }
    }

    // FIXME: describe me
    void embellish(Document document) {
        jsonld.embellish(document.data, getEmbellishData(document))
    }

    void setEmbellishLevels(List<String> embellishLevels) {
        this.embellishLevels = embellishLevels.collect()
    }

    void setFollowInverse(boolean followInverse) {
        this.followInverse = followInverse
    }

    void _setShouldFollowCategoryBroader(Function<String, Set<String>> getAllBroaderIds) {
        this._getAllBroaderIds = getAllBroaderIds
    }

    private List getEmbellishData(Document document) {
        if (document.getThingIdentifiers().isEmpty()) {
            return []
        }

        List<Map> start = [document.data]

        Set<String> visitedIris = new HashSet<>()
        visitedIris.addAll(plusWithoutHash(document.getThingIdentifiers()))

        // TODO? don't hardcode 'full'? could be 'cards' for index
        def docs = fetchIntegral('full', start, integral(getAllLinks(start)), visitedIris).collect()

        List result = docs.collect()
        Set<Link> links = getAllLinks(start + docs)
        
        // EXPERIMENTAL_CATEGORY_COLLECTION
        if (this._getAllBroaderIds) {
            links.findAll { it.property() == "category" }.forEach {
                links.addAll(_getAllBroaderIds.apply(it.iri).collect { new Link(iri: it, relation: "___") })
            }
        }

        Iterable<Map> previousLevelDocs = start + docs
        String previousLens = 'full'

        for (String lens : embellishLevels) {
            docs = fetchNonVisited(lens, uniqueIris(links), visitedIris)
            links = getAllLinks(docs)

            def integralDocs = fetchIntegral(lens, docs, integral(links), visitedIris)
            docs += integralDocs
            links += getAllLinks(integralDocs)

            if (followInverse) {
                previousLevelDocs.each {
                    def inverseDocs = insertInverse(previousLens, it, lens, visitedIris)
                    docs += inverseDocs
                    links += getAllLinks(inverseDocs)
                }
                previousLevelDocs = docs
                previousLens = lens
            }

            result += docs
        }
        if (followInverse) {
            // Last level: add reverse links, but don't include documents linking here in embellish graph
            previousLevelDocs.each { insertInverse(previousLens, it, null, visitedIris) }
        }

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

    private Iterable<Map> fetchNonVisited(String lens, Set<String> iris, Set<String> visitedIris) {
        def data = load(lens, iris - visitedIris)
        visitedIris.addAll(data.collectMany { plusWithoutHash(new Document(it).getThingIdentifiers()) })
        visitedIris.addAll(iris)
        return data
    }

    private Iterable<Map> fetchIntegral(String lens, Iterable<Map> docs, Set<Link> integralLinks, Set<String> visitedIris) {
        List<Map> result = []
        while(true) {
            var previousDocs = docs
            docs = fetchNonVisited(lens, uniqueIris(integralLinks), visitedIris)
            for (Map doc in previousDocs) {
                docs += insertInverseIntegral(lens, doc, lens, visitedIris)
            }
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

    private Iterable<Map> load(String lens, Set<String> iris) {
        if (iris.isEmpty()) {
            return []
        }

        def data = lens == 'full'
                ? getDocs.apply(iris.collect()) // NB! this collect must be here, see comment below!
                : getCards.apply(iris.collect()) // NB! this collect must be here, see comment below!

        
        // Without the .collect() above, this sometimes(!) fails with the following exception:
        //
        // 2021-12-17T19:16:20,484 [qtp184642382-23] ERROR whelk.rest.api.HttpTools - Internal server error: No signature of method: whelk.Whelk$_embellish_closure8.doCall() is applicable for argument types: (LinkedHashSet) values: [[http://kblocalhost.kb.se:5000/v8ncrvjbsh1z8fb2#it]]
        // Possible solutions: doCall(java.util.List), findAll(), findAll(), isCase(java.lang.Object), isCase(java.lang.Object)
        // groovy.lang.MissingMethodException: No signature of method: whelk.Whelk$_embellish_closure8.doCall() is applicable for argument types: (LinkedHashSet) values: [[http://kblocalhost.kb.se:5000/v8ncrvjbsh1z8fb2#it]]
        // Possible solutions: doCall(java.util.List), findAll(), findAll(), isCase(java.lang.Object), isCase(java.lang.Object)
        // at jdk.proxy1.$Proxy37.apply(Unknown Source) ~[?:?]
        // at java_util_function_Function$apply.call(Unknown Source) ~[?:?]
        // at whelk.Embellisher.load(Embellisher.groovy:149) ~[main/:?]

        if (lens == 'search-chips') {
            // NB! This depends on search-chips being subsets of cards. Since we shrink the cards to search-chips. 
            var searchChips = true
            data = data.collect{ (Map) jsonld.toChip(it, [] as Set, searchChips) }
        }
        else if (lens == 'chips') {
            data = data.collect{ (Map) jsonld.toChip(it) }
        }

        return data
    }

    private Iterable<Map> insertInverse(String forLens, Map thing, String applyLens, Set<String> visitedIris) {
        _insertInverse(forLens, thing, applyLens, visitedIris, false)
    }

    private Iterable<Map> insertInverseIntegral(String forLens, Map thing, String applyLens, Set<String> visitedIris) {
        _insertInverse(forLens, thing, applyLens, visitedIris, true)
    }

    private Iterable<Map> _insertInverse(String forLens, Map thing, String applyLens, Set<String> visitedIris, boolean onlyIntegral) {
        Set<String> inverseRelations = jsonld.getInverseProperties(thing, forLens)
        if (onlyIntegral) {
            inverseRelations = inverseIntegralRelations.intersect(inverseRelations) as Set
        } else {
            inverseRelations -= inverseIntegralRelations // they should already have been handled
        }
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