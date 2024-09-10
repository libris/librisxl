import whelk.search.ESQuery
import whelk.search.ElasticFind

var out = getReportWriter("ids")

var find = new ElasticFind(new ESQuery(getWhelk()))

var query = [
        '@type': ['Organization']
]

var t1 = System.currentTimeMillis()
/*find.findIds(query).forEach { id ->
    out.println(id)
}

 */
getWhelk().elastic.getPersonIds().forEach { id ->
    out.println(id)
}

var t2 = System.currentTimeMillis()
println("Took ${t2 - t1} ms")