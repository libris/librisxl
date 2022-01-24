/*
Link blank agents in <agent>.seeAlso if an exact match can be found. 

TODO: what to do about marc:controlSubfield:a / marc:controlSubfield:b

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