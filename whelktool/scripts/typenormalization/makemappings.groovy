import org.apache.jena.query.QueryExecutionFactory
import whelk.Document
import whelk.JsonLd
import whelk.util.Jackson

typeToCategoryQuery = """
prefix : <https://id.kb.se/vocab/>
prefix owl: <http://www.w3.org/2002/07/owl#>
select ?type ?cat {
  ?type owl:intersectionOf ( :Monograph [ owl:onProperty :category ; owl:hasValue ?cat ] ) .
}
"""

categoryMatchesQuery = """
prefix : <https://id.kb.se/vocab/>

prefix saogf: <https://id.kb.se/term/saogf/>
prefix tgm: <https://id.kb.se/term/gmgpc/swe/>
prefix kbrda: <https://id.kb.se/term/rda/>
prefix ktg: <https://id.kb.se/term/ktg/>
prefix marc: <https://id.kb.se/marc/>

select ?src ?tgt {
  {
    ?src a ?type ; (:exactMatch|:closeMatch|:broadMatch|:broader) ?tgt .
    values ?type {
      :Category
      :Genre
      :GenreForm
      :ContentType
      :CarrierType
    }
  } union {
    ?tgt :closeMatch|:exactMatch ?src .
    filter strstarts(str(?src), str(marc:))
  }
  filter(
    strstarts(str(?tgt), str(saogf:))
    || strstarts(str(?tgt), str(kbrda:))
    || strstarts(str(?tgt), str(tgm:))
    || strstarts(str(?tgt), str(ktg:))
  )
} order by ?src
"""

Map getMappings(String sparqlEndpoint, String query, srcKey, tgtKey) {
  var qe = QueryExecutionFactory.sparqlService(sparqlEndpoint, query)
  var res = qe.execSelect()
  var map = [:]
  var ids = res.collect {
    map[it.get(srcKey).toString()] = it.get(tgtKey).toString()
  }
  return map
}

Map makeMappings(String sparqlEndpoint) {

    var typeToCategory = getMappings(sparqlEndpoint, typeToCategoryQuery, 'type', 'cat')
    var categoryMatches = getMappings(sparqlEndpoint, categoryMatchesQuery, 'src', 'tgt')

    return [
        typeToCategory: typeToCategory,
        categoryMatches: categoryMatches
    ]
}

ofile = new File(args[0])
// NOTE: get from whelk.sparqlQueryClient.sparqlEndpoint (from secret.properties)
sparqlEndpoint = args[1]

Jackson.mapper.writerWithDefaultPrettyPrinter().writeValue(ofile, makeMappings(sparqlEndpoint))
