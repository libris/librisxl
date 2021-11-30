package whelk.external

import groovy.transform.Memoized
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.ResultSet
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.RDFNode
import whelk.component.ElasticSearch
import whelk.exception.WhelkRuntimeException
import whelk.util.Metrics
import groovy.util.logging.Log4j2 as Log

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration

import static whelk.util.Jackson.mapper

@Log
class Wikidata implements Mapper {
    Map<String, String> countryMap
    
    Wikidata(Map<String, String> countryMap) {
        this.countryMap = countryMap
        log.info("Initialized with ${countryMap.size()} country mappings")
    }
    
    @Override
    Optional<Map> getThing(String iri) {
        if (!isWikidata(iri)) {
            return Optional.empty()
        }

        WikidataEntity wdEntity = new WikidataEntity(iri, countryMap)

        return Optional.ofNullable(wdEntity.convert())
    }

    @Override
    boolean mightHandle(String iri) {
        return isWikidata(iri)
    }

    @Override
    String datasetId() {
        'https://id.kb.se/datasets/wikidata'
    }

    static boolean isWikidata(String iri) {
        iri.startsWith("https://www.wikidata.org") || iri.startsWith("http://www.wikidata.org")
    }

    static List<String> query(String query, String langTag, int limit) {
        try {
            performQuery(query, langTag, limit)
        }
        catch (Exception e) {
            throw new WhelkRuntimeException("Error querying wikidata: $e", e)
        }
    }

    /**
     * Search Wikidata using the wbsearchentities API
     * Documented here: https://www.wikidata.org/w/api.php?action=help&modules=wbsearchentities
     *
     * Language parameter: "Search in this language. This only affects how entities are selected, not 
     * the language in which the results are returned: this is controlled by the "uselang" parameter."
     *
     * @param query the query string
     * @param langTag language code for language to search in
     * @param limit max number of hits
     * @return a list of entity URIs
     */
    private static List<String> performQuery(String query, String langTag, int limit) {
        HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build()
        def base = 'https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json'
        def q = URLEncoder.encode(query, StandardCharsets.UTF_8)
        String uri = "$base&limit=$limit&language=$langTag&uselang=$langTag&search=$q"

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(uri))
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build()

        def httpResponse = client.send(request, HttpResponse.BodyHandlers.ofString())
        def result = mapper.readValue(httpResponse.body(), Map.class)
                .get('search')
                .collect { (String) it['concepturi'] }

        return result
    }
}

class WikidataEntity {
    static final String WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
    static final String WIKIDATA_ENTITY_NS = "http://www.wikidata.org/entity/"

    // Wikidata property short ids
    static final String COUNTRY = "P17"
    static final String DDC = "P1036"
    static final String EDITION = "P747"
    static final String END_TIME = "P582"
    static final String FAST = "P2163"
    static final String FREEBASE = "P646"
    static final String GEONAMES = "P1566"
    static final String GETTY = "P1667"
    static final String INSTANCE_OF = "P31"
    static final String LC_AUTH = "P244"
    static final String LOCATED_IN = "P131" // located in the administrative territorial entity
    static final String SUBCLASS_OF = "P279"
    static final String TORA = "P4820"
    static final String YSO = "P2347"

    // Wikidata class short ids
    static final String GEO_FEATURE = "Q618123"
    static final String HUMAN = "Q5"
    static final String SWEDISH_MUNI = "Q127448"
    static final String SWEDISH_COUNTY = "Q200547"

    enum KbvType {
        PLACE(GEO_FEATURE),
        PERSON(HUMAN),
        OTHER('')

        String wikidataType

        private KbvType(String wikidataType) {
            this.wikidataType = wikidataType
        }
    }

    Model graph

    String entityIri
    String shortId

    Map<String, String> countryMap
    
    WikidataEntity(String iri, Map<String, String> countryMap) {
        try {
            graph = ModelFactory.createDefaultModel()
            this.shortId = getShortId(iri)
            this.entityIri = WIKIDATA_ENTITY_NS + shortId
            loadGraph()
        }
        catch (ExceptionInInitializerError e) {
            e.printStackTrace()
        }
        this.countryMap = countryMap
    }

    private void loadGraph() {
        try {
            Metrics.clientTimer.labels(Wikidata.class.getSimpleName(), 'ttl-dump').time {
                graph.read("https://www.wikidata.org/wiki/Special:EntityData/${shortId}.ttl?flavor=dump", "Turtle")
            }
        } catch (Exception ex) {
            println("Unable to load graph for entity ${entityIri}")
        }
    }

    Map convert() {
        switch (type()) {
            case KbvType.PLACE: return convertPlace()
            case KbvType.PERSON: return convertPerson()
            default: return null
        }
    }

    Map convertPlace() {
        Map place =
                [
                        '@id'  : entityIri,
                        '@type': "Place"
                ]

        List prefLabel = getPrefLabel().findAll { it.getLanguage() in ElasticSearch.LANGUAGES_TO_INDEX }
        if (!prefLabel.isEmpty())
            place['prefLabelByLang'] = prefLabel.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        List description = getDescription().findAll { it.getLanguage() in ElasticSearch.LANGUAGES_TO_INDEX }
        if (!description.isEmpty())
            place['descriptionByLang'] = description.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        List country = getCountry().findAll { it.toString() != entityIri }
        if (!country.isEmpty())
            place['country'] = country.collect { ['@id': replaceIfCountry(it.toString())] }

        List locatedIn = getLocatedIn() - country
        if (!locatedIn.isEmpty())
            place['locatedIn'] = locatedIn.collect { ['@id': replaceIfCountry(it.toString())] }

        List ddc = getDdc().collect { code, edition ->
            Map bNode =
                    [
                            '@type': "ClassificationDdc",
                            'code' : code.toString()
                    ]
            if (edition)
                bNode['edition'] = ['@id': edition.toString()]

            return bNode
        }

        List lcsh = getLcsh().collect {
            ['@id': it.toString()]
        }

        List fast = getFast().collect {
            ['@id': it.toString()]
        }

        List getty = getGetty().collect {
            ['@id': it.toString()]
        }

        List closeMatches = ddc + lcsh + fast + getty

        if (closeMatches) {
            place['closeMatch'] = closeMatches
        }

        List identifiers = getPlaceIdentifiers()
        if (!identifiers.isEmpty())
            place['exactMatch'] = identifiers.collect { ['@id': it.toString()] }

        return place
    }
    
    String replaceIfCountry(String id) {
        return countryMap.get(id, id)
    }

    Map convertPerson() {
        Map person =
                [
                        '@id'  : entityIri,
                        '@type': "Person"
                ]

        List prefLabel = getPrefLabel().findAll { it.getLanguage() in ElasticSearch.LANGUAGES_TO_INDEX }
        if (!prefLabel.isEmpty())
            person['prefLabelByLang'] = prefLabel.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        return person
    }

    List<RDFNode> getPrefLabel() {
        String queryString = "SELECT ?prefLabel { wd:${shortId} skos:prefLabel ?prefLabel }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("prefLabel") }
    }

    List<RDFNode> getDescription() {
        String queryString = "SELECT ?description { wd:${shortId} sdo:description ?description }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("description") }
    }

    List<RDFNode> getCountry() {
        String queryString = "SELECT ?country { wd:${shortId} wdt:${COUNTRY} ?country }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("country") }
    }

    List<RDFNode> getLocatedIn() {
        String queryString = """
            SELECT DISTINCT ?place {
                wd:${shortId} p:${LOCATED_IN} ?stmt .
                ?stmt ps:${LOCATED_IN} ?place .
                FILTER NOT EXISTS { ?stmt pq:${END_TIME} ?endTime }
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("place") }
    }

    List<List<RDFNode>> getDdc() {
        String queryString = """
            SELECT ?code ?edition {
                wd:${shortId} wdt:${DDC} ?code ;
                  wdt:${DDC} ?stmt .
                OPTIONAL { ?stmt pq:${EDITION} ?edition }
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { [it.get("code"), it.get("edition")] }
    }

    List<RDFNode> getLcsh() {
        String queryString = """
            SELECT ?id {
                wd:${shortId} wdt:${LC_AUTH} ?shortId .
                bind(iri(concat("http://id.loc.gov/authorities/subjects/", ?shortId)) as ?id)
                FILTER(strstarts(?shortId, "sh"))
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("id") }
    }

    List<RDFNode> getFast() {
        String queryString = """
            SELECT ?fastId {
                wd:${shortId} wdtn:${FAST} ?fastId ;
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("fastId") }
    }

    List<RDFNode> getGetty() {
        String queryString = """
            SELECT ?fastId {
                wd:${shortId} wdtn:${GETTY} ?gettyId ;
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("gettyId") }
    }

    List<RDFNode> getPlaceIdentifiers() {
        String queryString = """
            SELECT ?freebaseId ?geonamesId ?toraId ?ysoId {
                VALUES ?place { wd:${shortId} }
                
                OPTIONAL { ?place wdtn:${FREEBASE} ?freebaseId }
                OPTIONAL { ?place wdtn:${GEONAMES} ?geonamesId }
                OPTIONAL { ?place wdt:${TORA} ?toraShortId }
                OPTIONAL { ?place wdtn:${YSO} ?ysoId }
                
                bind(iri(concat("https://data.riksarkivet.se/tora/", ?toraShortId)) as ?toraId)
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        QuerySolution singleRowResult = rs.next()

        return rs.getResultVars().findResults { singleRowResult?.get(it) }
    }

    KbvType type() {
        String queryString = "SELECT ?type { wd:${shortId} wdt:${INSTANCE_OF} ?type }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)
        Set wdTypes = rs.collect { it.get("type").toString() } as Set

        return KbvType.values().find { getSubclasses(it).intersect(wdTypes) } ?: KbvType.OTHER
    }

    @Memoized
    static Set<String> getSubclasses(KbvType type) {
        if (type == KbvType.OTHER) {
            return Collections.EMPTY_SET
        }

        String queryString = "SELECT ?class { ?class wdt:${SUBCLASS_OF}* wd:${type.wikidataType} }"

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WIKIDATA_ENDPOINT)

        return rs.collect { it.get("class").toString() }.toSet()
    }

    String getShortId(String iri) {
        iri.replaceAll(/.*\//, '')
    }
}

