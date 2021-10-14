package whelk.external

import groovy.transform.Memoized
import org.apache.jena.query.Query
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.ResultSet
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.RDFNode
import whelk.component.ElasticSearch

class Wikidata {
    Optional<Map> getThing(String iri) {
        if (!isWikidata(iri)) {
            return Optional.empty()
        }

        WikidataEntity wdEntity = new WikidataEntity(iri)

        return Optional.ofNullable(wdEntity.convert())
    }

    boolean isWikidata(String iri) {
        iri.startsWith("https://www.wikidata.org") || iri.startsWith("http://www.wikidata.org")
    }
}

class WikidataEntity {
    static final String WIKIDATA_ENDPOINT = "https://query.wikidata.org/sparql"
    static final String WIKIDATA_ENTITY_NS = "http://www.wikidata.org/entity/"

    static final String PROP_PREF_LABEL = "skos:prefLabel"
    static final String PROP_COUNTRY = "wdt:P17"
    static final String PROP_IS_PART_OF = "wdt:P131" // located in the administrative territorial entity

    enum Type {
        PLACE('Q618123'), // Geographical feature
        PERSON('Q5'), // Human
        OTHER('')

        String baseClass

        private Type(String baseClass) {
            this.baseClass = baseClass
        }
    }

    Model graph = ModelFactory.createDefaultModel()

    String entityIri
    String shortId

    WikidataEntity(String iri) {
        this.shortId = getShortId(iri)
        this.entityIri = WIKIDATA_ENTITY_NS + shortId
        loadGraph()
    }

    private void loadGraph() {
        try {
            graph.read("https://www.wikidata.org/wiki/Special:EntityData/${shortId}.ttl?flavor=dump", "Turtle")
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

        List prefLabel = getValuesOfProperty(PROP_PREF_LABEL).findAll { it.getLanguage() in ElasticSearch.LANGUAGES_TO_INDEX }
        if (!prefLabel.isEmpty())
            place['prefLabelByLang'] = prefLabel.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        List country = getValuesOfProperty(PROP_COUNTRY)
        if (!country.isEmpty())
            place['country'] = country.collect { ['@id': it.toString()] }

        List partOf = getValuesOfProperty(PROP_IS_PART_OF) - country
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

        List prefLabel = getValuesOfProperty(PROP_PREF_LABEL).findAll { it.getLanguage() in ElasticSearch.LANGUAGES_TO_INDEX }
        if (!prefLabel.isEmpty())
            person['prefLabelByLang'] = prefLabel.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        return person
    }

    Type type() {
        String command = "SELECT ?type { wd:${shortId} wdt:P31 ?type }"
        Query q = QueryRunner.prepareQuery(command)
        QueryExecution qExec = QueryRunner.localQueryExec(q, graph)
        ResultSet rs = QueryRunner.selectQuery(qExec)
        Set wdTypes = rs.collect { it.get("type").toString() } as Set

        return Type.values().find { getSubclasses(it).intersect(wdTypes) } ?: Type.OTHER
    }

    List<RDFNode> getValuesOfProperty(String prop) {
        String queryString = "SELECT ?o { wd:${shortId} ${prop} ?o }"

        Query q = QueryRunner.prepareQuery(queryString)
        QueryExecution qExec = QueryRunner.localQueryExec(q, graph)
        ResultSet rs = QueryRunner.selectQuery(qExec)

        return rs.collect { it.get("o") }
    }

    @Memoized
    static Set<String> getSubclasses(Type type) {
        if (type == Type.OTHER) {
            return Collections.EMPTY_SET
        }

        String queryString = "SELECT ?class { ?class wdt:P279* wd:${type.baseClass} }"
        Query q = QueryRunner.prepareQuery(queryString)
        QueryExecution qExec = QueryRunner.remoteQueryExec(q, WIKIDATA_ENDPOINT)
        ResultSet res = QueryRunner.selectQuery(qExec)

        return res.collect { it.get("class").toString() }.toSet()
    }

    String getShortId(String iri) {
        iri.replaceAll(/.*\//, '')
    }
}

