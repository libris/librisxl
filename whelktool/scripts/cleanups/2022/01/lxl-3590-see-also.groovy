/*
Link blank agents in <agent>.seeAlso if an exact match can be found. 

Agents with marc:controlSubfield: a/b ("earlier heading"/"later heading", "tidigare namn"/"senare namn")
will not match and are not handled. We might want to decide on a more specific relation for them.

   2601 marc:controlSubfield:a
   2535 marc:controlSubfield:b
      5 marc:controlSubfield:g
    292 marc:controlSubfield:i
      3 marc:controlSubfield:nnnc

https://www.loc.gov/marc/authority/adtracing.html
https://katalogverk.kb.se/katalogisering/Formathandboken/Auktoritetsformatet/Kontrollkoder-i-auktoritetsformatet/index.html

For more information, see LXL-3590
 */


import java.util.concurrent.LinkedBlockingQueue

linked = getReportWriter("linked.tsv")
notFound = getReportWriter("not-found.tsv")
multiple = getReportWriter("multiple-matches.tsv")
variant = getReportWriter("variant.txt")

selectByCollection('auth') { auth ->
    def (record, thing) = auth.graph
    if (!thing.seeAlso) {
        return
    }
    
    thing.seeAlso.each { seeAlso ->
        if (seeAlso.'@id') {
            return 
        }
        
        def ids = findIds(seeAlso).findAll { it != thing.'@id' }
        if (ids.size() == 1) {
            linked.println("${thing.'@id'}\t$seeAlso\t${ids.first()}")
            seeAlso.clear()
            seeAlso['@id'] = ids.first()
            auth.scheduleSave()
        }
        else {
            (ids ? multiple : notFound).println("${auth.doc.shortId}\t$seeAlso\t$ids")
        }
    }
}

def findIds(Map seeAlso) {
    def query = [
            'q' : [seeAlso.values().join(" ")]
    ]

    LinkedBlockingQueue<String> ids = new LinkedBlockingQueue<>() 
    selectByIds(queryIds(query).collect()) { candidate -> 
        def (record, candidateThing) = candidate.graph
        
        if (seeAlso.every { key, value -> candidateThing[key] == value}) {
            ids.add(candidateThing.'@id')
        }
        else if (candidateThing.hasVariant) {
            candidateThing.hasVariant.each {
                if (seeAlso.every { key, value -> it[key] == value} && it.every { key, value -> seeAlso[key] == value}) {
                    def allCaps = it.name && it.name.toUpperCase() == it.name
                    if (!allCaps) {
                        variant.println("$it")
                        ids.add(candidateThing.'@id')
                    }
                }
            }
            
        }
    }
    return ids.collect()
}
