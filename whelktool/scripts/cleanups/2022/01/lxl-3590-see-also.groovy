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

import java.util.concurrent.atomic.AtomicReference

linked = getReportWriter("linked.txt")
notLinked = getReportWriter("not-linked.txt")

selectByCollection('auth') { auth ->
    def (record, thing) = auth.graph
    if (thing.seeAlso) {
        thing.seeAlso.each { seeAlso ->
            if (!seeAlso.'@id') {
                def id = find(seeAlso) 
                if (id) {
                    linked.println("${auth.doc.shortId} $seeAlso -> $id")
                    seeAlso.clear()
                    seeAlso['@id'] = id
                    auth.scheduleSave()
                }
                else {
                    notLinked.println("${auth.doc.shortId} $seeAlso")
                }
            }
        }
    }
}

String find(Map seeAlso) {
    def query = [
            'q' : [seeAlso.values().join(" ")]
    ]

    AtomicReference<String> id = new AtomicReference<>() 
    selectByIds(queryIds(query).collect()) { candidate -> 
        def (record, candidateThing) = candidate.graph
        
        if (seeAlso.every { key, value -> candidateThing[key] == value}) {
            id.set(candidateThing.'@id')
        }
    }
    return id.get()
}