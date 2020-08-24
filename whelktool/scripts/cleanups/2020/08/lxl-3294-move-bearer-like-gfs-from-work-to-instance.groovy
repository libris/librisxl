/**
 * Move bearer-like genre/form terms from work to instance.
 *
 * See LXL-3294 for more information.
 */

gfsToMove = [
        "E-böcker",
        "E-single",
        "E-textbok",
        "Electronic books",
        "Ljudbok cd",
        "Ljudbok kassett",
        "Ljudbok mp3",
        "Musik cd",
        "Tal cd",
        "Talbok Daisy",
        "Talbok kassett",
        "Video dvd",
        "Video vhs"]

class Script {
    static PrintWriter report
}
Script.report = getReportWriter("report.txt")

selectByCollection('bib') { bib ->
    try {
        process(bib)
    }
    catch(Exception e) {
        System.err.println("${bib.doc.shortId} $e")
        e.printStackTrace()
    }
}

void process(bib) {
    def (record, thing) = bib.graph
    Map work = getWork(thing)

    if (!work || !work.genreForm) {
        return
    }

    def gf = work.genreForm

    gfsToMove.each {
        def toMove = [:]
        if (gf instanceof Map && gf.prefLabel == it) {
            toMove = gf
            work.remove("genreForm")
        }

        if (gf instanceof List) {
            toMove = gf.find {
                g -> g.prefLabel == it
            }
            gf.remove(toMove)
            if (gf.isEmpty()) {
                work.remove("genreForm")
            }
        }

        if (!toMove) {
            return
        }
        if (thing.genreForm) {
            if (thing.genreForm instanceof Map) {
                thing.genreForm = [thing.genreForm]
            }
            thing.genreForm.add(toMove)
        } else {
            thing["genreForm"] = toMove
        }

        bib.scheduleSave()
        Script.report.println("${bib.doc.shortId} ${thing.genreForm} ${work.genreForm}")
    }
}

Map getWork(def thing) {
    if(thing && thing['instanceOf'] && thing['instanceOf']['@type']) {
        return thing['instanceOf']
    }
    return null
}