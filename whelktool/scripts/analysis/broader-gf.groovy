import whelk.util.DocumentUtil
import whelk.util.Statistics

s = new Statistics(5).printOnShutdown()

selectByCollection('bib') { bib ->
    def work = getWork(bib)

    if(!work) {
        return
    }

    if(work['genreForm']) {
        List<String> ids = work['genreForm']['@id']
        if (ids.size() > 1) {
            [ids, ids].combinations{ a,b ->
                if (a != b) {
                    check(bib.whelk, a, b)
                    check(bib.whelk, b, a)
                }
            }
        }
    }
}

void check(whelk, String a, String b) {
    if (whelk.relations.isImpliedBy(a, b)) {
        s.increment(a, b)
        s.increment('#broader', a)
    }
}

Map getWork(def bib) {
    def (record, thing, work) = bib.graph
    if (thing && isInstanceOf(thing, 'Work')) {
        return thing
    }
    else if(thing && thing['instanceOf'] && isInstanceOf(thing['instanceOf'], 'Work')) {
        return thing['instanceOf']
    }
    else if (work && isInstanceOf(work, 'Work')) {
        return work
    }
    return null
}