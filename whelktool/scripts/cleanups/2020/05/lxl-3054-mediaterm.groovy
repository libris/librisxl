/**
 * Parse subtitle and responsibilityStatement from malformed marc:mediaterm created by
 * failed conversion from MARC21.
 *
 * See LXL-3054 for more information
 */

import whelk.util.DocumentUtil
import whelk.util.Statistics

class Script {
    static Statistics s = new Statistics()
    static Map map = [
            // Order is significant. Will be matched in this order.
            'responsibilityStatement': ['/ ', '/c ', '/ c ', '/$c', '/'],
            'subtitle': [':'],
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
        Script.s.increment('FAILED', e.getMessage())
        Script.s.increment('TOTAL', 'FAILED')
    }
}

boolean handle(bib, misplaced, StringBuilder msg) {
    if (isPunctuationOnly(misplaced)) {
        Script.s.increment('DELETED', misplaced)
        Script.s.increment('TOTAL', 'DELETED')
        msg.append("DELETED")
        return true
    }

    for (def e in Script.map) {
        def field = e.getKey()
        for (prefix in e.getValue()) {
            if (misplaced.startsWith(prefix)) {
                set(bib, field, misplaced.substring(prefix.size()), msg)
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
            put(bib, instance, field, repairBrackets(value.trim()), msg)
            break;

        case 'subtitle':
            if (value.indexOf('/') != -1) {
                value.split('/').with { x ->
                    set(bib, 'subtitle', x[0], msg)
                    set(bib, 'responsibilityStatement', x[1], msg)
                }
            }
            else {
                Map title = instance['hasTitle']?.find{ it['@type'] == 'Title' && it['mainTitle'] }
                if(!title) {
                    println("${bib.doc.getURI()} Could not set subtitle: No 'Title', hasTitle: ${instance['hasTitle']}\n")
                    throw new RuntimeException("Could not set subtitle")
                }
                put(bib, title, field, repairBrackets(value.trim()), msg)
            }
            break;
    }
}

void put(bib, thing, field, value, msg) {
    if (thing[field] && !((String) thing[field]).isAllWhitespace() && thing[field] != value) {
        println("${bib.doc.getURI()} Already has $field, new: $value, existing:${thing[field]}\n")
        throw new RuntimeException("Already has other value in $field")
    }
    thing[field] = value

    msg.append("${field}:${value} ")
    Script.s.increment('TOTAL', field)
    Script.s.increment("Y - $field", value)
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