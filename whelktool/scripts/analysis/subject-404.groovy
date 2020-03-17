import whelk.util.DocumentUtil
import whelk.util.Statistics

class Script {
    static Statistics s = new Statistics().printOnShutdown()
}

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println(e)
        e.printStackTrace()
    }

}

void process(bib) {
    def (record, thing) = bib.graph

    Map work = thing['instanceOf']

    if(!work) {
        return
    }

    if(work['subject']) {
        for (Map subject in (work['subject'] as List<Map>)) {
            if(subject['@type'] != 'ComplexSubject') {
                if (subject['sameAs'] && subject['sameAs'][0] && subject['sameAs'][0]['@id'] && subject['sameAs'][0]['@id'].contains('id.kb.se')) {
                    Script.s.increment('sameAs', subject['sameAs'][0]['@id'], bib.doc.shortId)
                }
            }
        }

    }
}