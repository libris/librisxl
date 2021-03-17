import whelk.datatool.DocumentItem

List activeInOldDb = new File(scriptDir, "active-in-old-db.csv").readLines()
List shouldBeActive = new File(scriptDir, "should-be-active.csv").readLines()

List<Map> shelfMarkSeqData = // Lägg in korrekt löpnummer för dessa fem signumsviter
        [
                ["label": "SLF", "nextShelfControlNumber": 1, "description": "Svensk liggfolio"],
                ["label": "SLF", "nextShelfControlNumber": 1, "description": "Svensk liggfolio, broschyrer", "qualifier": "br"],
                ["label": "LF", "nextShelfControlNumber": 1, "description": "Utländsk liggfolio"],
                ["label": "öh", "nextShelfControlNumber": 1, "description": "Övrig hemlig"],
                ["label": "öh F", "nextShelfControlNumber": 1, "description": "Övrig hemlig • Folio"]
        ]

List shelfMarkSeqsAsRecords = []

activeInOldDb.each { row ->
    List cols = row.split(",")

    // Active shelf mark sequence as described in old db
    String prefixBeforeYear = cols[0]
    String prefixAfterYear = cols[1]
    String year = cols[3]
    int lastSerialNumber = year == "2021" ? cols[2] as int : 0 // Some have not (yet) been used in 2021

    // See if sequence from old db is actually in given list with active sequences
    String shouldBeActiveShelfMarkSeq = shouldBeActive.find {
        String completeLabel = it.split(",")[0]
        String beforeYear = completeLabel.replaceAll("2021.*", "")
        String afterYear = completeLabel.replaceAll(".*2021", "")

        beforeYear == prefixBeforeYear && afterYear == prefixAfterYear
    }

    // Not all sequences marked as active in old db should be included (as per given list)
    if (!shouldBeActiveShelfMarkSeq)
        return

    // Properties that should be included for new ShelfMarkSequence
    String label = prefixBeforeYear + "2021" + prefixAfterYear
    String nextShelfControlNumber = lastSerialNumber + 1
    List descriptionParts = shouldBeActiveShelfMarkSeq.split(",")
    String description = descriptionParts[1] + " • " + descriptionParts[2]
    String qualifier

    // Move shelf control number prefix from label to separate property 'qualifier'
    if (label =~ / br$/) {
        label = label.replace(" br", "")
        qualifier = "br"
    }

    if (label =~ / Diss$/) {
        label = label.replace(" Diss", "")
        qualifier = "Diss"
    }

    Map props = [:]

    props["label"] = label
    props["nextShelfControlNumber"] = nextShelfControlNumber
    props["description"] = description
    if (qualifier)
        props["qualifier"] = qualifier

    shelfMarkSeqData << props
}

shelfMarkSeqData.each { data ->
    Map shelfMarkSeq =
            ["@graph": [
                    [
                            "@id"       : "TEMPID",
                            "@type"     : "Record",
                            "mainEntity": ["@id": "TEMPID#it"]
                    ],
                    [
                            "@id"            : "TEMPID#it",
                            "@type"          : "ShelfMarkSequence",
                            "shelfMarkStatus": "ActiveShelfMark"
                    ]
            ]]

    shelfMarkSeq["@graph"][1] += data

    DocumentItem newShelfMarkSeq = create(shelfMarkSeq)

    shelfMarkSeqsAsRecords << newShelfMarkSeq
}

selectFromIterable(shelfMarkSeqsAsRecords) { sms ->
    sms.scheduleSave()
}

// Set descriptionCreator to the correct sigel
selectBySqlWhere("collection = 'auth' AND data#>>'{@graph,1,@type}' = 'ShelfMarkSequence'") { sms ->
    sms.graph[0]["descriptionCreator"] = ["@id": "https://libris.kb.se/library/S"]
    sms.scheduleSave()
}

// Link holdings to the newly created sequences
shelfMarkSeqsAsRecords.each { sms ->
    String shelfMarkSeqId = sms.graph[1]["@id"]
    String shelfMarkSeqLabel = sms.graph[1]["label"]
    String shelfMarkSeqQualifier = sms.graph[1]["qualifier"]

    selectBySqlWhere("collection = 'hold' AND data#>>'{@graph,1,hasComponent}' LIKE '%\"${shelfMarkSeqLabel}\"%'") { hold ->
        boolean modified

        hold.graph[1]["hasComponent"].each { comp ->
            Object label = comp.shelfMark?.label
            String shelfMarkLabelInHold = label instanceof List ? label[0] : label
            String shelfControlNumberInHold = comp["shelfControlNumber"]
            boolean hasQualifier = shelfControlNumberInHold =~ /br .+|Diss .+/

            if (shelfMarkLabelInHold == shelfMarkSeqLabel) {
                if (!shelfMarkSeqQualifier && !hasQualifier) {
                    comp["shelfMark"] = ["@id": shelfMarkSeqId]
                    modified = true
                    return
                }
                if (shelfMarkSeqQualifier && shelfControlNumberInHold?.startsWith(shelfMarkSeqQualifier)) {
                    comp["shelfMark"] = ["@id": shelfMarkSeqId]
                    modified = true
                }
            }
        }

        if (modified) {
            hold.scheduleSave()
        }
    }
}
