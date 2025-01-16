/**
 * Copied/adapted from: whelktool/scripts/2021/06/lxl-3526-link-agents-to-wikidata.groovy for repeat-running
 */

import org.apache.jena.query.ARQ
import org.apache.jena.query.ParameterizedSparqlString
import org.apache.jena.query.QueryExecution
import org.apache.jena.query.QueryExecutionFactory
import org.apache.jena.query.QuerySolution
import org.apache.jena.query.ResultSet
import org.apache.jena.query.ResultSetFactory
import org.apache.jena.shared.PrefixMapping

import java.text.Normalizer

Map multiplePropsMatchAndNoMismatch = Collections.synchronizedMap([:])
Map multiplePropsMatchIncludingIsniOrViafAndNoMismatch = Collections.synchronizedMap([:])
Map onePropMatchAndNoMismatch = Collections.synchronizedMap([:])
Map mismatch = Collections.synchronizedMap([:])
List multipleAgentsMatch = Collections.synchronizedList([])

Map wdDataByLibrisId = getAgentDataFromWikidata()

selectByIds(wdDataByLibrisId.keySet()) { data ->
    Map instance = data.graph[1]
    String librisId = data.doc.shortId

    Map librisProps = getPropertiesFromLibris(instance)
    Map wdData = wdDataByLibrisId[librisId]

    List matchedWdAgents = []

    wdData.each { String wdUri, Map wdProps ->
        List matchedProps = []
        List mismatchedProps = []

        wdProps.each { String propName, Set wdValues ->
            Collection librisValues = librisProps[propName]
            if (librisValues) {
                if (librisValues.any { String lVal ->
                    wdValues.any { String wdVal ->
                        matchingValues(propName, lVal, wdVal)
                    }
                })
                    matchedProps << propName
                else
                    mismatchedProps << propName
            }
        }

        matchedProps.sort()
        mismatchedProps.sort()

        if (mismatchedProps) {
            Map propMap = ["matched": matchedProps, "mismatched": mismatchedProps]
            mismatch[propMap] = mismatch[propMap] ?: []
            mismatch[propMap] << [librisId, wdUri]
        } else if (matchedProps.size() == 1) {
            onePropMatchAndNoMismatch[matchedProps] = onePropMatchAndNoMismatch[matchedProps] ?: []
            onePropMatchAndNoMismatch[matchedProps] << [librisId, wdUri]
        } else if (matchedProps.size() > 1) {
            if ("viaf" in matchedProps || "isni" in matchedProps) {
                multiplePropsMatchIncludingIsniOrViafAndNoMismatch[matchedProps] =
                        multiplePropsMatchIncludingIsniOrViafAndNoMismatch[matchedProps] ?: []
                multiplePropsMatchIncludingIsniOrViafAndNoMismatch[matchedProps] << [librisId, wdUri]

                matchedWdAgents << wdUri
            }
            else {
                multiplePropsMatchAndNoMismatch[matchedProps] = multiplePropsMatchAndNoMismatch[matchedProps] ?: []
                multiplePropsMatchAndNoMismatch[matchedProps] << [librisId, wdUri]
            }
        }
    }

    if (matchedWdAgents) {
        if (matchedWdAgents.size() > 1) {
            multipleAgentsMatch << [librisId] + matchedWdAgents
        } else {
            Map entityLink = ["@id": matchedWdAgents[0]]
            instance["exactMatch"] = instance.exactMatch ?: []
            if (!(entityLink in instance.exactMatch)) {
                instance.exactMatch << entityLink
                data.scheduleSave()
            }
        }
    }
}

// Changed from orginal, we no longer want reports:
/*println("Writing reports...")
printReport(multiplePropsMatchIncludingIsniOrViafAndNoMismatch, getReportWriter("multiple-props-match-incl-viaf-or-isni.txt"))
printReport(multiplePropsMatchAndNoMismatch, getReportWriter("multiple-props-match.txt"))
printReport(onePropMatchAndNoMismatch, getReportWriter("one-prop-match.txt"))
printReport(mismatch, getReportWriter("mismatches.txt"))
printReport(multipleAgentsMatch, getReportWriter("multiple-agents-match.txt"))*/

Map getAgentDataFromWikidata() {
    Map agentDataByLibrisId = [:]

    ARQ.init()

    // Retrieving all data at once causes query time-out, start with the URIs only
    List agents = findAgentsInWikidata()

    // Then retrieve properties for each agent
    // Again we can not retrieve all data with a single query
    // Whereas performing one query per resource takes a lot of time
    // Split in batches to make it faster:
    int batchSize = 1000
    int counter = 0
    while (agents) {
        List batch = agents.take(batchSize)
        agentDataByLibrisId += processWikidataBatch(batch)
        agents = agents.drop(batchSize)
        counter += batch.size()
        println("Wikidata resources processed: " + counter)
    }

    return agentDataByLibrisId
}

List findAgentsInWikidata() {
    // wdt:P5587 = Libris-URI, wdt:P31 = instance of, wd:Q5 = human
    String command = """
        SELECT ?wdUri WHERE {
            ?wdUri wdt:P5587 ?librisId ;
                wdt:P31 wd:Q5 .
        }
    """

    ResultSet resultSet = runQuery(command)

    List agents = []

    while (resultSet.hasNext()) {
        QuerySolution binding = resultSet.next()
        agents << binding.get("wdUri")
    }

    println("Found " + agents.size() + " Wikidata resources linked to libris agents")

    return agents
}

Map processWikidataBatch(List wikidataAgents) {
    int batchSize = wikidataAgents.size()

    // The str function removes language tags so we get only unique names (not e.g. "Astrid Lindgren"@en, "Astrid Lindgren"@sv etc.)
    String command = """
        SELECT DISTINCT ?agent ?librisId (str(?label) AS ?name) (year(?birthDate) as ?birthYear) (year(?deathDate) as ?deathYear) ?viaf ?isni WHERE {
            VALUES ?agent { ${"? " * batchSize}} .
            ?agent wdt:P5587 ?librisId .
            OPTIONAL { ?agent rdfs:label|skos:altLabel ?label }
            OPTIONAL { ?agent wdt:P569 ?birthDate }
            OPTIONAL { ?agent wdt:P570 ?deathDate }
            OPTIONAL { ?agent wdt:P214 ?viaf }
            OPTIONAL { ?agent wdt:P213 ?isni }
        }
    """

    ResultSet resultSet = runQuery(command, wikidataAgents)

    Map wdDataByLibrisId = [:]
    List props = resultSet.getResultVars() - ["librisId", "agent"]

    while (resultSet.hasNext()) {
        QuerySolution binding = resultSet.next()

        // Most libris ids in Wikidata are new and short form, e.g. wf7g6ht72hd73zb
        String librisId = binding.get("librisId").getLexicalForm()
        // But a few are legacy ids, e.g. http://libris.kb.se/bib/17260747
        if (librisId.contains(":")) {
            String canonicalId = findCanonicalId(librisId)
            if (canonicalId) {
                librisId = canonicalId - baseUri - "#it"
            }
        }
        String agent = binding.get("agent").toString()

        if (!wdDataByLibrisId[librisId])
            wdDataByLibrisId[librisId] = [:]
        if (!wdDataByLibrisId[librisId][agent])
            wdDataByLibrisId[librisId][agent] = [:]

        Map agentProps = wdDataByLibrisId[librisId][agent]

        props.each { p ->
            String pValue = binding.get(p)?.getLexicalForm()
            if (pValue) {
                pValue = p == "isni" ? pValue.split().join() : p == "name" ? normalizeString(pValue).toLowerCase() : pValue

                if (agentProps[p] in Set)
                    agentProps[p] << pValue
                else
                    agentProps[p] = [pValue] as Set
            }
        }
    }

    return wdDataByLibrisId
}

ResultSet runQuery(String command, List values = null) {
    PrefixMapping prefixes = PrefixMapping.Factory.create()
            .setNsPrefix("rdfs", "http://www.w3.org/2000/01/rdf-schema#")
            .setNsPrefix("skos", "http://www.w3.org/2004/02/skos/core#")
            .setNsPrefix("wd", "http://www.wikidata.org/entity/")
            .setNsPrefix("wdt", "http://www.wikidata.org/prop/direct/")

    ParameterizedSparqlString paramString = new ParameterizedSparqlString(command, prefixes)
    values?.eachWithIndex { v, i ->
        paramString.setParam(i, v)
    }

    QueryExecution qExec = QueryExecutionFactory.sparqlService("https://query.wikidata.org/sparql", paramString.asQuery())

    ResultSet resultSet

    try {
        ResultSet results = qExec.execSelect()
        resultSet = ResultSetFactory.copyResults(results)
    } catch (Exception ex) {
        System.out.println(ex.getMessage())
    } finally {
        qExec.close()
    }

    return resultSet
}

Map getPropertiesFromLibris(Map instance) {
    Map props = [:]

    Set names = []
    Set birthYears = []
    Set deathYears = []

    ([instance.subMap("name", "givenName", "familyName", "lifeSpan")] + instance.hasVariant).findAll().each { Map variant ->
        if (variant.name)
            names << normalizeString(variant.name).toLowerCase()
        if (variant.givenName && variant.familyName)
            names << normalizeString(variant.givenName + " " + variant.familyName).toLowerCase()
        // lifeSpan requires a bit of parsing to get same form as from Wikidata
        if (variant.lifeSpan) {
            List fromTil = variant.lifeSpan.split(/-+/)

            // Skip e.g. "1200-talet" and "1200-talet-1300-talet", not very helpful
            // However e.g. "1220 eller 1221-1290 eller 1291" is of interest
            if (fromTil.size() < 3 && fromTil.every { it =~ /\d/ }) {
                def (birth, death) = fromTil

                List by = birth.split(/\D+/).findAll().collect {
                    // x + 1 years b.c. -> -x years
                    if (variant.lifeSpan =~ /(?i)f..?kr|b.c|π.Χ/)
                        return (-it.toInteger() + 1).toString()
                    else
                        return it
                }
                birthYears += by

                if (death) {
                    List dy = death.split(/\D+/).findAll().collect {
                        if (death =~ /(?i)f..?kr|b.c|π.Χ/)
                            return (-it.toInteger() + 1).toString()
                        else
                            return it
                    }
                    deathYears += dy
                }
            }
        }
    }

    if (names)
        props["name"] = names
    if (birthYears)
        props["birthYear"] = birthYears
    if (deathYears)
        props["deathYear"] = deathYears

    List viaf = instance.identifiedBy?.findResults { it."@type" == "VIAF" && it."value" ? it."value".replaceAll(/\W/, '') : null }
    if (viaf)
        props["viaf"] = viaf

    List isni = instance.identifiedBy?.findResults { it."@type" == "ISNI" && it."value" ? it."value".replaceAll(/\W/, '') : null }
    if (isni)
        props["isni"] = isni

    return props
}

boolean matchingValues(String prop, String a, String b) {
    if (prop == "name") {
        List aParts = a.split()
        List bParts = b.split()

        return aParts.containsAll(bParts) || bParts.containsAll(aParts)
    }

    return a == b
}

String normalizeString(String s) {
    return Normalizer.normalize(s, Normalizer.Form.NFD).replaceAll(/\p{M}/, '').replaceAll(/\p{Lm}|\P{L}/, ' ')
}

void printReport(Object data, PrintWriter report) {
    if (data in Map) {
        data.sort { a, b ->
            b.value.size() <=> a.value.size()
        }.each { props, resources ->
            String category = props in Map
                    ? "Mismatched on: ${props.mismatched.join(', ')}\nMatched on: ${props.matched.join(', ')}"
                    : props.join(', ')
            String numInCat = "(${resources.size()})"

            report.println(category)
            report.println(numInCat)
            report.println("-" * numInCat.size())

            resources.each {
                report.println(it.join(" • "))
            }
            report.println()
        }
    } else {
        data.each {
            report.println(it.join(" • "))
        }
    }
}

