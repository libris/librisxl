PrintWriter failedBibIDs = getReportWriter("failed-to-update-bibIDs")
PrintWriter scheduledForUpdate = getReportWriter("scheduled-for-update")

selectBySqlWhere("", silent: false, { bib ->
    scheduledForUpdate.println("${bib.doc.getURI()}")

    // FROMTERM
    // TOTERM
    /*
    @graph,1,instanceOf
                    "subject": [
                    {
                        "@id": "https://id.kb.se/term/sao/1940-talet"
                    },
                    {
                        "@type": "ComplexSubject",
                        "sameAs": [
                            {
                                "@id": "https://id.kb.se/term/sao/S%C3%A4kerhetspolitik--historia"
                            }
                        ],
                        "inScheme": {
                            "@id": "https://id.kb.se/term/sao"
                        },
                        "prefLabel": "S\u00e4kerhetspolitik--historia",
                        "termComponentList": [
                            {
                                "@type": "Topic",
                                "prefLabel": "S\u00e4kerhetspolitik"
                            },
                            {
                                "@type": "TopicSubdivision",
                                "prefLabel": "historia"
                            }
                        ]
                    },

     */
    //List subjects = bib.graph[1].instanceOf.subject

    bib.graph[1].instanceOf.subject.each { subj ->
        if (subj["@type"] == "ComplexSubject") {

        }
    }



    bib.scheduleSave(loud: true, onError: { e ->
        failedBibIDs.println("Failed to update ${bib.doc.shortId} due to: $e")
    })
})
