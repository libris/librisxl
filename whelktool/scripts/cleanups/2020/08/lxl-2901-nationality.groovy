/**
 * Fix broken nationality links in agents.
 *
 * See LXL-2901 for more information.
 */

import groovy.transform.Memoized
import whelk.util.DocumentUtil
import whelk.util.Statistics

class Script {
    static Statistics s = new Statistics().printOnShutdown()
    static String ID_KB = 'https://id.kb.se/nationality/'
    static PrintWriter report

    static Map codeMap = [
            'a-tu'    : 'a-tu---',
            'a-uz'    : 'a-uz---',
            'da'      : 'e-dk---',
            'e--sw---': 'e-sw---',
            'e-et---' : 'f-et---',
            'e-ev---' : 'ev-----',
            'e-it'    : 'e-it---',
            'e-li'    : 'e-li---',
            'e-sw'    : 'e-sw---',
            'e-sw--'  : 'e-sw---',
            'e-sw----': 'e-sw---',
            'e-swe---': 'e-sw---',
            'e-xxk,'  : 'e-uk---',
            'f-tr---' : 'f-ti---',
            'fr'      : 'e-fr---',
            'gw'      : 'e-gw---',
            'it'      : 'e-it---',
            'n-xxu'   : 'n-us---',
            'pl'      : 'e-pl---',
            'sp'      : 'e-sp---',
            'sw'      : 'e-sw---',
            'u-uz---' : 'a-uz---',
            'xxc'     : 'n-cn---',
            'xxk'     : 'e-uk---',
            'xxu'     : 'n-us---',
            'xxx'     : 'xx',
    ]
}
Script.report = getReportWriter("report.txt")

selectByCollection('auth') { auth ->
    try {
        process(auth)
    }
    catch(Exception e) {
        System.err.println("${auth.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(auth) {
    def (_record, thing) = auth.graph

    DocumentUtil.findKey(thing, '@id') { value, path ->
        if (value.contains('nationality') && is404(value)) {
            String replacement = map(value)
            if(replacement) {
                auth.scheduleSave()
                Script.report.println("${auth.doc.shortId} ${value} -> ${replacement}")
                Script.s.increment('mapped', value)
                return new DocumentUtil.Replace(replacement)
            }
            else {
                Script.report.println("${auth.doc.shortId} ${value} -> NOT MAPPED")
                Script.s.increment('not mapped', value)
                return DocumentUtil.NOP
            }
        }
    }
}

String map(String link) {
    if (link.startsWith(Script.ID_KB)) {
        String code = link.substring(Script.ID_KB.size())
        if (Script.codeMap.containsKey(code)) {
            return Script.ID_KB + Script.codeMap[code]
        }
    }

    return null
}

@Memoized
boolean is404(String link) {
    boolean result = true
    selectByIds([link]) {
        result = false
    }
    return result
}
