import org.apache.jena.query.QueryExecutionFactory
import whelk.Document
import whelk.JsonLd
import whelk.util.Jackson

cleanupTypes = [
  'ProjectedImageInstance': ['ProjectedImage'],
  'MovingImageInstance': ['MovingImage'],
  'KitInstance': ['Kit'],
  'NotatedMusicInstance': ['NotatedMusic'],
  'TextInstance': ['Text', ['Volume', 'Electronic']],
  'StillImageInstance': ['StillImage', ['Sheet', 'DigitalResource']],
  'GlobeInstance': ['CartographicObject', 'PhysicalObject']
]

typeToCategoryQuery = """
prefix : <https://id.kb.se/vocab/>
prefix owl: <http://www.w3.org/2002/07/owl#>
select ?type ?cat {
  ?type owl:intersectionOf ( :Monograph [ owl:onProperty :category ; owl:hasValue ?cat ] ) .
} order by ?type ?cat
"""

preferredCategoryQuery = """
prefix : <https://id.kb.se/vocab/>

prefix saogf: <https://id.kb.se/term/saogf/>
prefix tgm: <https://id.kb.se/term/gmgpc/swe/>
prefix kbrda: <https://id.kb.se/term/rda/>
prefix ktg: <https://id.kb.se/term/ktg/>
prefix marc: <https://id.kb.se/marc/>

select ?src ?tgt {
  ?src (:closeMatch|:exactMatch)|^(:closeMatch|:exactMatch) ?tgt .
  filter(strstarts(str(?src), str(marc:)) || strstarts(str(?src), str(tgm:)))
  filter(
    strstarts(str(?tgt), str(saogf:))
    || strstarts(str(?tgt), str(kbrda:))
    || strstarts(str(?tgt), str(tgm:))
    || strstarts(str(?tgt), str(ktg:))
  )
} order by ?src ?tgt
"""

categoryMatchesQuery = """
prefix : <https://id.kb.se/vocab/>

prefix saogf: <https://id.kb.se/term/saogf/>
prefix tgm: <https://id.kb.se/term/gmgpc/swe/>
prefix kbrda: <https://id.kb.se/term/rda/>
prefix ktg: <https://id.kb.se/term/ktg/>
prefix marc: <https://id.kb.se/marc/>

select ?src ?bdr {
  ?src a ?type ; :closeMatch|:broadMatch|:broader ?bdr .
  values ?type {
    :Category
    :Genre
    :GenreForm
    :ContentType
    :CarrierType
  }
  filter(
    strstarts(str(?bdr), str(saogf:))
    || strstarts(str(?bdr), str(kbrda:))
    || strstarts(str(?bdr), str(tgm:))
    || strstarts(str(?bdr), str(ktg:))
  )
} order by ?src ?bdr
"""

Map getMappings(String sparqlEndpoint, String query, srcKey, tgtKey, list=false) {
  var qe = QueryExecutionFactory.sparqlService(sparqlEndpoint, query)
  var res = qe.execSelect()
  var map = [:]
  var ids = res.collect {
    var key = it.get(srcKey).toString()
    var value = it.get(tgtKey).toString()
    if (map.containsKey(key)) {
      def current = map.get(key)
      if (current !instanceof List) current = [current]
      if (value !in current) current << value
      if (!list && current.size() == 1) current = current[0]
      map[key] = current
    } else {
      map[key] = list ? [value] : value
    }
  }
  return map
}

Map makeMappings(String sparqlEndpoint) {

    var typeToCategory = getMappings(sparqlEndpoint, typeToCategoryQuery, 'type', 'cat')
    var preferredCategory = getMappings(sparqlEndpoint, preferredCategoryQuery, 'src', 'tgt')
    var categoryMatches = getMappings(sparqlEndpoint, categoryMatchesQuery, 'src', 'bdr', true)

    return [
        cleanupTypes: cleanupTypes,
        typeToCategory: typeToCategory,
        preferredCategory: preferredCategory,
        categoryMatches: categoryMatches,
    ]
}

ofile = new File(args[0])
// NOTE: get from whelk.sparqlQueryClient.sparqlEndpoint (from secret.properties)
sparqlEndpoint = args[1]

Jackson.mapper.writerWithDefaultPrettyPrinter().writeValue(ofile, makeMappings(sparqlEndpoint))
