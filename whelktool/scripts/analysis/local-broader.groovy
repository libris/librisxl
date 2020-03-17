/**
 * Find unlinked 'broader'
 *
 * See LXL-3213 for more information.
 */


import groovy.transform.Memoized
import whelk.util.DocumentUtil

class Script {
    static PrintWriter report
    static PrintWriter selfRef
    static PrintWriter is404
    static PrintWriter error
}
Script.report = getReportWriter("report.txt")
Script.selfRef = getReportWriter("self-ref.txt")
Script.error = getReportWriter("error.txt")
Script.is404 = getReportWriter("404.txt")

selectByCollection('auth') { auth ->
    try {
        process(auth)
    }
    catch(Exception e) {
        //Script.error.
        println("${auth.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(auth) {
    Map thing = auth.graph[1]
    String id = thing['@id']
    List broader = thing['broader']

    if (!broader) {
        return
    }

    broader.findAll{ !it['@id'] }.each { Map b ->
        Script.report.println("$id $b")
    }

    broader.findAll{ id == it['@id'] }.each { Map b ->
        Script.selfRef.println("$id")
    }
    broader.findAll{ it['@id'] && is404(it['@id']) }.each { Map b ->
        Script.is404.println("$id $b")
    }
}

@Memoized
boolean is404(String id) {
    Map thing = null
    selectByIds([id]) { auth ->
        thing = auth.graph[1]
    }
    return thing == null
}