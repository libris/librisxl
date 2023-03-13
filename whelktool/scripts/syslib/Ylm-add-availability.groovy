String where = """id in (
   SELECT lh.id FROM lddb lh WHERE
   lh.data #>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Ylm'
   AND lh.deleted = 'false')
"""

selectBySqlWhere(where, silent: false, { hold ->

    def holds = hold.graph[1]
    boolean shouldChange = false
    boolean modified = false

    if (holds["hasComponent"]) {

        holds["hasComponent"].each { it ->
            if (it["availability"]) {
                asList(it["availability"]).each { availability ->

                    if (availability["label"] && availability["label"] instanceof String) {
                        if (availability["label"].contains("Ej fjärrlån")) {
                            shouldChange = false
                        }
                    }
                }
            } else {
                shouldChange = true
                it["availability"] = []

            }

            if (shouldChange) {
                modified = true
                it["availability"] <<
                        [
                                "@id": "https://id.kb.se/term/enum/NotForILL"
                        ]
            }
        }
    }

    if (!holds["hasComponent"]) {
        boolean change = false

        if (!holds["availability"]) {
            modified = true
            holds["availability"] = []
            holds["availability"] <<
                    [
                            "@id": "https://id.kb.se/term/enum/NotForILL"
                    ]
        }

        if (holds["availability"]) {

            asList(holds["availability"]).each { avail ->
                if (avail["label"] && avail["label"] instanceof String) {
                    if (!avail["label"].contains("Ej fjärrlån")) {
                        change = true
                    }
                }
            }
            if (change) {
                modified = true
                holds["availability"] <<
                        [
                                "@id": "https://id.kb.se/term/enum/NotForILL"
                        ]
            }
        }
    }

    if (modified) {
        hold.scheduleSave(loud: true)
    }

})
