List tif = new File(scriptDir, "suecia-tif.csv").readLines()
List pdf = new File(scriptDir, "suecia-pdf.csv").readLines()

Map mappings = Collections.synchronizedMap([:])
List newRecords = Collections.synchronizedList([])

tif.drop(1).each { row ->
    List cols = row.split(/,(?!.\.tif)/)

    String physicalId = cols[0]
    if (physicalId == "saknas")
        return
    String digitalId = cols[1]
    String webUrn = cols[4]
    String tifMaster = cols[5].replaceAll("\"", "")
    String tifMaster2 = cols[6].replaceAll("\"", "")

    if (digitalId == "saknas") {
        mappings[physicalId] =
                ["webUrn"    : webUrn,
                 "tifMaster" : tifMaster,
                 "tifMaster2": tifMaster2]
    } else {
        mappings[physicalId] =
                ["digitalId" : digitalId,
                 "webUrn"    : webUrn,
                 "tifMaster" : tifMaster,
                 "tifMaster2": tifMaster2]
    }
}

pdf.drop(1).each {
    List row = it.split(",")
    String physicalId = row[0]
    String datakbsePdf = row[4]

    if (mappings[physicalId])
        mappings[physicalId]["datakbsePdf"] = datakbsePdf
}

String controlNumbersPhysical = "('" + mappings.collect { it.key }.join("','") + "')"

Set keys = Collections.synchronizedSet([] as Set)

selectBySqlWhere("collection = 'bib' AND data#>>'{@graph,0,controlNumber}' IN $controlNumbersPhysical") { physical ->
    Map physicalRecord = physical.graph[0]
    Map physicalInstance = physical.graph[1]

    String physicalControlNumber = physicalRecord.controlNumber
    String physicalMainEntityId = physicalInstance."@id"

    Map mapping = mappings.remove(physicalControlNumber)

    // If a digital version already exists, update hash table to facilitate linking later
    if (mapping.digitalId) {
        mapping["physicalMainEntityId"] = physicalMainEntityId
        String digitalId = mapping.remove("digitalId")
        mappings[digitalId] = mapping
        return
    }

    // Template
    Map digiObject =
            ["@graph": [
                    [
                            "@id"       : "TEMPID",
                            "@type"     : "Record",
                            "mainEntity": ["@id": "TEMPID#it"]
                    ],
                    [
                            "@id"  : "TEMPID#it",
                            "@type": "Electronic"
                    ]
            ]]

    Map digiRecord = digiObject["@graph"][0]
    Map digiInstance = digiObject["@graph"][1]

    // Add bibliography
    digiRecord["bibliography"] =
            [
                    [
                            "@type": "Library",
                            "sigel": "DIGI"
                    ]
            ]
    digiRecord.bibliography += physicalRecord.bibliography.findAll { it.sigel in ["SAH", "SAHT", "SAHF"] }

    // Add properties from physical instance
    digiInstance += physicalInstance.subMap(
            [
                    "instanceOf",
                    "issuanceType",
                    "hasTitle",
                    "responsibilityStatement",
                    "publication",
                    "extent"
            ]
    )
    if (physicalInstance.identifiedBy)
        digiInstance["indirectlyIdentifiedBy"] = physicalInstance.identifiedBy

    // Add digi-specific properties
    digiInstance["reproductionOf"] = ["@id": physicalMainEntityId]
    digiInstance["mediaType"] = ["@id": "https://id.kb.se/term/rda/Computer"]
    digiInstance["carrierType"] =
            [
                    ["@id": "https://id.kb.se/term/rda/OnlineResource"],
                    ["@id": "https://id.kb.se/marc/Online"],
                    ["@id": "https://id.kb.se/marc/OnlineResource"]
            ]
    digiInstance["production"] = // Datum?
            [
                    [
                            "@type"   : "Reproduction",
                            "agent"   : [
                                    "@type": "Agent",
                                    "label": ["Kungliga biblioteket"]
                            ],
                            "place"   : [
                                    [
                                            "@type": "Place",
                                            "label": ["Stockholm"]
                                    ]
                            ],
                            "typeNote": "Digitalt faksimil"
                    ]
            ]

    // Add associated media either from physical instance or, if missing, from given tables
    if (physicalInstance.associatedMedia) {
        digiInstance["associatedMedia"] = physicalInstance.associatedMedia
    } else {
        digiInstance["associatedMedia"] =
                [
                        [
                                "@type"          : "MediaObject",
                                "cataloguersNote": ["sueciapdf"],
                                "marc:publicNote": [
                                        "pdf",
                                        "Fritt tillgänglig via Kungl. biblioteket"
                                ],
                                "uri"            : [
                                        mapping.webUrn
                                ]
                        ],
                        [
                                "@type"          : "MediaObject",
                                "cataloguersNote": ["datapdf"],
                                "marc:publicNote": [
                                        "pdf",
                                        "Fritt tillgänglig via Kungl. biblioteket"
                                ],
                                "uri"            : [
                                        mapping.datakbsePdf
                                ]
                        ],
                        [
                                "@type"          : "MediaObject",
                                "cataloguersNote": ["master"],
                                "marc:publicNote": [
                                        "Masterfil",
                                        "Fritt tillgänglig via Kungl. biblioteket"
                                ],
                                "uri"            : [
                                        mapping.tifMaster
                                ]
                        ]
                ]

        if (mapping.tifMaster2)
            digiInstance["associatedMedia"] <<
                    [
                            "@type"          : "MediaObject",
                            "cataloguersNote": ["master"],
                            "marc:publicNote": [
                                    "Masterfil",
                                    "Fritt tillgänglig via Kungl. biblioteket"
                            ],
                            "uri"            : [
                                    mapping.tifMaster2
                            ]
                    ]
    }

    newRecords << create(digiObject)
}

selectFromIterable(newRecords) {
    it.scheduleSave()
}

String controlNumbersDigital = "('" + mappings.collect { it.key }.join("','") + "')"

selectBySqlWhere("collection = 'bib' AND data#>>'{@graph,0,controlNumber}' IN $controlNumbersDigital") { digital ->
    String controlNumber = digital.graph[0].controlNumber

    digital.graph[1]["reproductionOf"] = ["@id": mappings[controlNumber].physicalMainEntityId]
    digital.graph[1]["@type"] = "Electronic"

    digital.scheduleSave()
}

