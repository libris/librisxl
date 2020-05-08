/**
 * Parse subTitle and responsibilityStatement from malformed marc:mediaterm created by
 * failed conversion from MARC21.
 *
 * See LXL-3054 for more information
 */

import whelk.util.DocumentUtil
import whelk.util.Statistics

class Script {
    static Statistics s = new Statistics()
    static Map map = [
            'responsibilityStatement': ['/ ', '/', '/c ', '/$c'],
            'subTitle': [':', ': '],
    ]
}

Script.s.printOnShutdown()
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectByCollection('bib') { bib ->
    try {
        StringBuilder msg = new StringBuilder()
        boolean modified = DocumentUtil.findKey(bib.doc.data, 'marc:mediaTerm') { value, path ->
            if (value instanceof List) {
                return DocumentUtil.NOP // There are a couple of these but none containing title information
            }

            if (value.contains(']')) {
                String mediaType = value.substring(0, value.indexOf(']'))
                String suffix = value.substring(value.indexOf(']') + 1).trim()
                if (!suffix.isBlank()) {
                    if(handle(bib, suffix, msg.append("${bib.doc.getURI()} $suffix -> "))) {
                        return new DocumentUtil.Replace(mediaType)
                    }
                }
            }
            return DocumentUtil.NOP
        }

        if (modified) {
            scheduledForUpdate.println(msg.toString())
            bib.scheduleSave()
        }
    }
    catch(Exception e) {
        println(e)
        e.printStackTrace()
        Script.s.increment('failed', e.getMessage())
    }
}

boolean handle(bib, misplaced, StringBuilder msg) {
    if (isPunctuationOnly(misplaced)) {
        Script.s.increment('DELETED', misplaced)
        Script.s.increment('TOTAL', 'DELETED')
        msg.append("DELETED")
        return true
    }

    Script.map.each { field, prefix ->
        prefix.each {
            if (misplaced.startsWith(it)) {
                set(bib, field, misplaced.substring(it.size()), msg)
                return true
            }
        }
    }

    Script.s.increment('Z - unhandled', misplaced)
    return false
}

void set(bib, String field, String value, StringBuilder msg) {
    if (isPunctuationOnly(value)) {
        return
    }
    Map instance = instance(bib)

    switch (field) {
        case 'responsibilityStatement':
            if (instance[field] && !((String) instance[field]).isAllWhitespace()) {
                throw new RuntimeException("Already has $field")
            }

            instance[field] = repairBrackets(value.trim())
            break;

        case 'subTitle':
            Map title = instance['hasTitle']?.find {it['@type'] == 'Title' && it['mainTitle'] && !it['subTitle']}
            if(!title) {
                throw new RuntimeException("Could not set subtitle")
            }

            if (value.indexOf('/') != -1) {
                value.split('/').with { x ->
                    title['subTitle'] = value.trim(x[0])
                    set(bib, 'responsibilityStatement', x[1])
                }
            }
            else {
                title['subTitle'] = repairBrackets(value.trim())
            }

            break;
    }

    msg.append("${field}:${value} ")
    Script.s.increment('TOTAL', 'field')
}

Map instance(bib) {
    bib.graph[1]
}

String repairBrackets(String s) {
    hasUnclosedBracket(s)
            ? s + ']'
            : s
}

boolean hasUnclosedBracket(String s) {
    int numOpen = s.findAll(/\[/).size()
    int numClose = s.findAll(/\]/).size()
    return numOpen == numClose + 1
}

boolean isPunctuationOnly(String s) {
    !s.find(/[\p{L}\d]/)
}