package whelk.external

import groovy.transform.Memoized
import org.apache.jena.query.ResultSet
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.RDFNode
import whelk.component.ElasticSearch
import whelk.exception.WhelkRuntimeException
import whelk.util.Metrics

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Duration

import static whelk.util.Jackson.mapper

class Wikidata implements Mapper {
    @Override
    Optional<Map> getThing(String iri) {
        if (!isWikidata(iri)) {
            return Optional.empty()
        }

        WikidataEntity wdEntity = new WikidataEntity(iri)

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
    
    static List<String> query(String query) {
        try {
            performQuery(query)
        }
        catch (Exception e) {
            throw new WhelkRuntimeException("Error querying wikidata: $e", e)
        }
    }

    private static List<String> performQuery(String query) {
        HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NORMAL).build()
        def base = 'https://www.wikidata.org/w/api.php?action=wbsearchentities&format=json'
        def lang = 'sv'
        def limit = 5
        def q = URLEncoder.encode(query, StandardCharsets.UTF_8)
        String uri = "$base&limit=$limit&language=$lang&uselang=$lang&search=$q"
        
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
    static final String END_TIME = "P582"
    static final String INSTANCE_OF = "P31"
    static final String PART_OF_PLACE = "P131" // located in the administrative territorial entity
    static final String SUBCLASS_OF = "P279"

    enum Type {
        PLACE('Q618123'), // Geographical feature
        PERSON('Q5'), // Human
        OTHER('')

        String baseClass

        private Type(String baseClass) {
            this.baseClass = baseClass
        }
    }

    Model graph

    String entityIri
    String shortId

    WikidataEntity(String iri) {
        try {
            graph = ModelFactory.createDefaultModel()
            this.shortId = getShortId(iri)
            this.entityIri = WIKIDATA_ENTITY_NS + shortId
            loadGraph()
        }
        catch (ExceptionInInitializerError e) {
            e.printStackTrace()
        }
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
            case Type.PLACE: return convertPlace()
            case Type.PERSON: return convertPerson()
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
        if (!prefLabel.isEmpty())
            place['descriptionByLang'] = description.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        List country = getCountry().findAll { it.toString() != entityIri }
        if (!country.isEmpty())
            place['country'] = country.collect { ['@id': it.toString()] }

        List partOf = getPartOfPlace() - country
        if (!partOf.isEmpty())
            place['isPartOf'] = partOf.collect { ['@id': it.toString()] }

        return place
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

    List<RDFNode> getPartOfPlace() {
        String queryString = """
            SELECT ?place { 
                wd:${shortId} p:${PART_OF_PLACE} ?stmt .
                ?stmt ps:${PART_OF_PLACE} ?place .
                FILTER NOT EXISTS { ?stmt pq:${END_TIME} ?endTime }
            }
        """

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)

        return rs.collect { it.get("place") }
    }

    Type type() {
        String queryString = "SELECT ?type { wd:${shortId} wdt:${INSTANCE_OF} ?type }"

        ResultSet rs = QueryRunner.localSelectResult(queryString, graph)
        Set wdTypes = rs.collect { it.get("type").toString() } as Set

        return Type.values().find { getSubclasses(it).intersect(wdTypes) } ?: Type.OTHER
    }

    @Memoized
    static Set<String> getSubclasses(Type type) {
        if (type == Type.OTHER) {
            return Collections.EMPTY_SET
        }

        String queryString = "SELECT ?class { ?class wdt:${SUBCLASS_OF}* wd:${type.baseClass} }"

        ResultSet rs = QueryRunner.remoteSelectResult(queryString, WIKIDATA_ENDPOINT)

        return rs.collect { it.get("class").toString() }.toSet()
    }

    String getShortId(String iri) {
        iri.replaceAll(/.*\//, '')
    }
}

