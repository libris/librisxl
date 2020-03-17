import whelk.util.DocumentUtil
import datatool.util.Statistics

Statistics s = new Statistics()
s.printOnShutdown()

selectByCollection('bib') { bib ->
    try {
        DocumentUtil.findKey(bib.doc.data, 'role') { Object value, path ->
            count(s, value)
        }
    }
    catch(Exception e) {
        println(e)
        e.printStackTrace()
    }
}


private String normalize(String s) {
    def noise = [",", '"', "'", '[', ']', ',', '.', '.', ':', ';', '-', '(', ')', '-', '–', '+', '!', '?'].collectEntries { [it, ''] }
    return s.toLowerCase().replace(noise).trim()
}

void count(Statistics s, Object role) {
    if (role instanceof Map && !role['@id']) {
        count1(s, role, 'code')
        count1(s, role, 'label')
    }
    else if (role instanceof String) {
        s.increment('string', role.toString())
    }
    else if (role instanceof List) {
        s.increment('list size', role.size())
        role.each { count(s, it) }
    }
}

void count1(Statistics s, Map thing, String prop) {
    if (thing[prop]) {
        s.increment(prop, normalize(thing[prop].toString()))
    }
}