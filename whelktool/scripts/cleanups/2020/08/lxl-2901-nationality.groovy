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
            '--sw---'  : 'e-sw---',
            '-e-sp---' : 'e-sp---',
            '-e-sw---' : 'e-sw---',
            '-sw---'   : 'e-sw---',
            'a-ii'     : 'a-ii---',
            'a-sw---'  : 'e-sw---',
            'a-swe---' : 'e-sw---',
            'a-tu'     : 'a-tu---',
            'a-us---'  : 'n-us---',
            'a-uz'     : 'a-uz---',
            'ar'       : 's-ag---',
            'at'       : 'u-at---',
            'au'       : 'e-au---',
            'be'       : 'e-be---',
            'cc'       : 'a-cc---',
            'cz'       : 'e-xr---',
            'da'       : 'e-dk---',
            'de'       : 'e-gx---',
            'dk'       : 'e-dk---',
            'e---sw'   : 'e-sw---',
            'e--es'    : 'e-sp---',
            'e--fr'    : 'e-fr---',
            'e--gr'    : 'e-gr---',
            'e--sw'    : 'e-sw---',
            'e--sw---' : 'e-sw---',
            'e--swe'   : 'e-sw---',
            'e--swe---': 'e-sw---',
            'e--uk'    : 'e-uk---',
            'e-aa--'   : 'e-aa---',
            'e-bu--'   : 'e-bu---',
            'e-dk--'   : 'e-dk---',
            'e-es---'  : 'e-sp---',
            'e-et---'  : 'f-et---',
            'e-ev---'  : 'ev-----',
            'e-fi'     : 'e-fi---',
            'e-fi--'   : 'e-fi---',
            'e-fr--'   : 'e-fr---',
            'e-gb---'  : 'e-uk---',
            'e-gx'     : 'e-gx---',
            'e-gx--'   : 'e-gx---',
            'e-it'     : 'e-it---',
            'e-it--'   : 'e-it---',
            'e-li'     : 'e-li---',
            'e-ne'     : 'e-ne---',
            'e-nl---'  : 'e-ne---',
            'e-no--'   : 'e-no---',
            'e-pol---' : 'e-pl---',
            'e-ru--'   : 'e-ru---',
            'e-se---'  : 'e-sw---',
            'e-sp--'   : 'e-sp---',
            'e-sv--'   : 'e-sw---',
            'e-sw'     : 'e-sw---',
            'e-sw-'    : 'e-sw---',
            'e-sw--'   : 'e-sw---',
            'e-sw----' : 'e-sw---',
            'e-swe'    : 'e-sw---',
            'e-swe-'   : 'e-sw---',
            'e-swe--'  : 'e-sw---',
            'e-swe---' : 'e-sw---',
            'e-swe----': 'e-sw---',
            'e-ua---'  : 'e-un---',
            'e-uk--'   : 'e-uk---',
            'e-us---'  : 'n-us---',
            'e-xx---'  : 'xx',
            'e-xxk,'   : 'e-uk---',
            'e-xxk---' : 'e-uk---',
            'e-xxu---' : 'n-us---',
            'e-xxx---' : 'xx',
            'ee'       : 'e-er---',
            'ee-sw---' : 'e-sw---',
            'ew-sw---' : 'e-sw---',
            'f-mv---'  : 'f-mw---',
            'f-tr---'  : 'f-ti---',
            'fi'       : 'e-fi---',
            'fr'       : 'e-fr---',
            'gw'       : 'e-gw---',
            'hu'       : 'e-hu---',
            'il'       : 'a-is---',
            'in'       : 'a-ii---',
            'ir'       : 'a-ir---',
            'it'       : 'e-it---',
            'ita'      : 'e-it---',
            'jp'       : 'a-ja---',
            'n-ca---'  : 'n-cn---',
            'n-us'     : 'n-us---',
            'n-us--'   : 'n-us---',
            'n-xxc---' : 'n-cn---',
            'n-xxu'    : 'n-us---',
            'n-xxu---' : 'n-us---',
            'ne'       : 'e-ne---',
            'nl'       : 'e-ne---',
            'no'       : 'e-no---',
            'nr'       : 'f-nr---',
            'nwmj---'  : 'nwjm---',
            'pl'       : 'e-pl---',
            'ro'       : 'e-rm---',
            'ru'       : 'e-ru---',
            's-sw---'  : 'e-sw---',
            'se'       : 'e-sw---',
            'sp'       : 'e-sp---',
            'sw'       : 'e-sw---',
            'sw---'    : 'e-sw---',
            'sw-e--'   : 'e-sw---',
            'sw-e---'  : 'e-sw---',
            'sw-se---' : 'e-sw---',
            'swe'      : 'e-sw---',
            'swe--'    : 'e-sw---',
            'swx'      : 'e-sw---',
            'sz'       : 'e-sz---',
            'u-uz---'  : 'a-uz---',
            'w-sw--'   : 'e-sw---',
            'w-sw---'  : 'e-sw---',
            'xxc'      : 'n-cn---',
            'xxk'      : 'e-uk---',
            'xxq'      : 'n-us---',
            'xxu'      : 'n-us---',
            'xxx'      : 'xx',
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
