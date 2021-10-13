package whelk.external

import groovy.transform.Memoized
import org.apache.jena.query.Query
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.ResultSet
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.rdf.model.RDFNode

class Wikidata {
    Optional<Map> getThing(String iri) {
        if (!isWikidataEntity(iri)) {
            return Optional.empty()
        }

        WikidataEntity wdEntity = new WikidataEntity(iri)

        return Optional.ofNullable(wdEntity.convert())
    }

    boolean isWikidataEntity(String iri) {
        iri.startsWith("https://www.wikidata.org")
    }
}

class WikidataEntity {
    static final String wikidataEndpoint = "https://query.wikidata.org/sparql"

    enum Properties {
        PREF_LABEL('skos:prefLabel'),
        COUNTRY('wdt:P17'),
        PART_OF('wdt:P131') // located in the administrative territorial entity

        String prefixedIri

        private Properties(String prefixedIri) {
            this.prefixedIri = prefixedIri
        }
    }

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

    String iri
    String shortId

    WikidataEntity(String iri) {
        this.iri = iri
        this.shortId = getShortId(iri)
        loadGraph()
    }

    private void loadGraph() {
        try {
            graph.read("https://www.wikidata.org/wiki/Special:EntityData/${shortId}.rdf?flavor=dump")
        } catch (Exception ex) {
            println("Unable to load graph for entity ${iri}")
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
                        '@id'  : iri, // Måste vara entity irin!
                        '@type': "Place"
                ]

        List langsOfInterest = ['sv', 'en']
        List prefLabel = getValuesOfProperty(Properties.PREF_LABEL.prefixedIri)
        List prefLabelsOfInterest = prefLabel.findAll { it.getLanguage() in langsOfInterest }
        if (!prefLabelsOfInterest.isEmpty())
            place['prefLabelByLang'] = prefLabelsOfInterest.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

        List country = getValuesOfProperty(Properties.COUNTRY.prefixedIri)
        if (!country.isEmpty())
            place['country'] = country.collect { ['@id': it.toString()] }

        List partOf = getValuesOfProperty(Properties.PART_OF.prefixedIri) - country
        if (!partOf.isEmpty())
            place['isPartOf'] = partOf.collect { ['@id': it.toString()] }

        return place
    }

    Map convertPerson() {
        Map person =
                [
                        '@id'  : iri,
                        '@type': "Person"
                ]

        List langsOfInterest = ['sv', 'en']
        List prefLabel = getValuesOfProperty(Properties.PREF_LABEL.prefixedIri)
        List prefLabelsOfInterest = prefLabel.findAll { it.getLanguage() in langsOfInterest }
        if (!prefLabelsOfInterest.isEmpty())
            person['prefLabelByLang'] = prefLabelsOfInterest.collectEntries { [it.getLanguage(), it.getLexicalForm()] }

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
        QueryExecution qExec = QueryRunner.remoteQueryExec(q, wikidataEndpoint)
        ResultSet res = QueryRunner.selectQuery(qExec)
        return res.collect { it.get("class").toString() }.toSet()
    }

    String getShortId(String iri) {
        iri.replaceAll(/.*\//, '')
    }
}

