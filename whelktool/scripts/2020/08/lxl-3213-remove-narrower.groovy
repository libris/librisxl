/**
 * Fix broken nationality links in agents.
 *
 * See LXL-2901 for more information.
 */

import whelk.util.DocumentUtil

class Script {
    static PrintWriter report
    static PrintWriter noBroader
    static PrintWriter error
}
Script.report = getReportWriter("report.txt")
Script.noBroader = getReportWriter("no-broader.txt")
Script.error = getReportWriter("error.txt")

selectByCollection('auth') { auth ->
    try {
        process(auth)
    }
    catch(Exception e) {
        Script.error.println("${auth.doc.shortId} $e")
        e.printStackTrace(Script.error)
    }
}

void process(auth) {
    Map thing = auth.graph[1]
    String id = thing['@id']
    List narrower = thing['narrower']

    if (!narrower) {
        return
    }

    if (narrower.removeAll { it['@id'] && hasBroader(it['@id'], id) } ) {
        if (narrower.isEmpty()) {
            thing.remove('narrower')
        }

        Script.report.println(auth.doc.shortId)
        auth.scheduleSave()
    }

    narrower.findResults{ it['@id'] }.each { String narrowerId ->
        Map n = getThing(narrowerId)
        Script.noBroader.println("""
                ${auth.doc.shortId} 
                ${thing['prefLabel']} <-- ${n['prefLabel']} 
                $id <-- $narrowerId
                actual broader: ${n['broader']}
                \n""".stripIndent()
        )
    }
}

boolean hasBroader(String narrower, String broader) {
    return (getThing(narrower)['broader'] ?: []).collect{ it['@id'] }.contains(broader)
}

Map getThing(String id) {
    Map thing = null
    selectByIds([id]) { auth ->
        thing = auth.graph[1]
    }
    if (!thing) {
        throw new RuntimeException("$id does not exist")
    }
    return thing
}
