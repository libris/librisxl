TEST_CASES = [
    {
        "shb_rec": {
            "@graph": [
                {
                    "recordStatus": "marc:CorrectedOrRevised",
                    "encodingLevel": "marc:LessThanFullLevelMaterialNotExamined",
                    "descriptionConventions": [
                        {"@id": "https://id.kb.se/marc/CatFormType-a"}
                    ],
                    "modified": "2023-05-09T16:42:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/28",
                    "@type": "Record",
                    "mainEntity": {"@id": "https://libris-qa.kb.se/dataset/shb/28#it"},
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/28#28",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/3685700"},
                        "item": "28",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/28#28"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/28#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                    },
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Curt Weibull",
                            "subtitle": "en bibliografi 19 augusti 1976",
                        }
                    ],
                    "responsibilityStatement": "Larsson, Gunilla",
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": "Stockholm"}],
                            "year": "1976",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": "27 s."}],
                    "seriesMembership": [
                        {
                            "inSeries": {
                                "@type": "PhysicalResource",
                                "category": [
                                    {"@id": "https://id.kb.se/term/saobf/Print"}
                                ],
                                "hasTitle": [
                                    {
                                        "@type": "Title",
                                        "mainTitle": "Acta Bibliothecae regiae Stockholmiensis",
                                    }
                                ],
                                "identifiedBy": [
                                    {"@type": "ISSN", "value": "0065-1060"}
                                ],
                            },
                            "seriesEnumeration": "28",
                        }
                    ],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": "Fullständig beskrivning (OCR) ur SHBD: Larsson, Gunilla, Curt Weibull : en bibliografi 19 augusti 1976. -Stockholm, 1976. - 27 s. - (Acta Bibliothecae regiae Stockholmiensis, ISSN 0065-1060 ; 28)",
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/m4xtxx0z02xdx6w",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/8369159"}],
                    "created": "2002-01-13T00:00:00+01:00",
                    "modified": "2025-11-28T15:05:03.427+01:00",
                    "mainEntity": {"@id": "https://libris.kb.se/m4xtxx0z02xdx6w#it"},
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "917000059X"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "8369159",
                    "encodingLevel": "marc:FullLevel",
                    "technicalNote": [
                        {"@type": "TechnicalNote", "label": ["Recensionslänk"]}
                    ],
                    "generationDate": "2026-03-05T09:08:16.394+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/me/devops/globalchanges-PROD/librisxl/whelktool/reports/prod-20260305-082639/restore/restore.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/NB"},
                    "descriptionLastModifier": {
                        "@id": "https://libris.kb.se/library/Li"
                    },
                },
                {
                    "@id": "https://libris.kb.se/m4xtxx0z02xdx6w#it",
                    "@type": "PhysicalResource",
                    "extent": [{"@type": "Extent", "label": ["27, [1] s."]}],
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/8369159"}],
                    "category": [
                        {"@id": "https://id.kb.se/term/saobf/Print"},
                        {"@id": "https://id.kb.se/term/rda/Volume"},
                    ],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "subtitle": "en bibliografi 19 augusti 1976",
                            "mainTitle": "Curt Weibull",
                        }
                    ],
                    "instanceOf": {
                        "@type": "Monograph",
                        "subject": [
                            {"@id": "https://libris.kb.se/pm135z271l26txm#it"},
                            {
                                "@type": "ComplexSubject",
                                "inScheme": {"@id": "https://id.kb.se/term/sao"},
                                "termComponentList": [
                                    {"@id": "https://id.kb.se/term/sao/Historiker"},
                                    {
                                        "@type": "GeographicSubdivision",
                                        "prefLabel": "Sverige",
                                    },
                                    {
                                        "@type": "TemporalSubdivision",
                                        "prefLabel": "1800-talet",
                                    },
                                    {
                                        "@type": "TemporalSubdivision",
                                        "prefLabel": "1900-talet",
                                    },
                                ],
                            },
                        ],
                        "category": [
                            {"@id": "https://id.kb.se/marc/Bibliography"},
                            {"@id": "https://id.kb.se/term/saogf/Bibliografier"},
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/swe"}],
                        "contribution": [
                            {
                                "role": {"@id": "https://id.kb.se/relator/author"},
                                "@type": "PrimaryContribution",
                                "agent": {
                                    "@id": "https://libris.kb.se/hftw0kb13x86bx4#it"
                                },
                            }
                        ],
                        "classification": [
                            {
                                "code": "Aalz Weibull, Curt",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F5/"}
                                    ],
                                    "version": "5",
                                },
                            }
                        ],
                    },
                    "manufacture": [
                        {
                            "@type": "Manufacture",
                            "agent": {"@type": "Agent", "label": ["Bohusläningen"]},
                            "place": [{"@type": "Place", "label": ["Uddevalla"]}],
                        }
                    ],
                    "publication": [
                        {
                            "year": "1976",
                            "@type": "PrimaryPublication",
                            "agent": {"@type": "Agent", "label": ["Kungl. bibl."]},
                            "place": [{"@type": "Place", "label": ["Stockholm"]}],
                            "country": {"@id": "https://id.kb.se/country/sw"},
                        }
                    ],
                    "identifiedBy": [
                        {
                            "@type": "ISBN",
                            "value": "917000059X",
                            "acquisitionTerms": ["35:00"],
                        }
                    ],
                    "hasDimensions": {"@type": "Dimensions", "label": ["24 cm"]},
                    "seriesMembership": [
                        {
                            "@type": "SeriesMembership",
                            "inSeries": {
                                "@type": "Instance",
                                "instanceOf": {
                                    "@type": "Work",
                                    "hasTitle": [
                                        {
                                            "@type": "Title",
                                            "mainTitle": "Acta Bibliothecae regiae Stockholmiensis",
                                        }
                                    ],
                                },
                                "identifiedBy": [
                                    {"@type": "ISSN", "value": "0065-1060"}
                                ],
                            },
                            "seriesEnumeration": "28",
                        }
                    ],
                    "responsibilityStatement": "red. av Gunilla Larsson",
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": True,
        "description": "Expected match.",
    },
    {
        "shb_rec": {
            "@graph": [
                {
                    "recordStatus": "marc:CorrectedOrRevised",
                    "encodingLevel": "marc:LessThanFullLevelMaterialNotExamined",
                    "descriptionConventions": [
                        {"@id": "https://id.kb.se/marc/CatFormType-a"}
                    ],
                    "modified": "2023-05-09T16:42:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/28",
                    "@type": "Record",
                    "mainEntity": {"@id": "https://libris-qa.kb.se/dataset/shb/28#it"},
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/28#28",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/3685700"},
                        "item": "28",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/28#28"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/28#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                    },
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Curt Weibull",
                            "subtitle": "en bibliografi 19 augusti 1976",
                        }
                    ],
                    "responsibilityStatement": "Larsson, Gunilla",
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": "Stockholm"}],
                            "year": "1976",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": "27 s."}],
                    "seriesMembership": [
                        {
                            "inSeries": {
                                "@type": "PhysicalResource",
                                "category": [
                                    {"@id": "https://id.kb.se/term/saobf/Print"}
                                ],
                                "hasTitle": [
                                    {
                                        "@type": "Title",
                                        "mainTitle": "Acta Bibliothecae regiae Stockholmiensis",
                                    }
                                ],
                                "identifiedBy": [
                                    {"@type": "ISSN", "value": "0065-1060"}
                                ],
                            },
                            "seriesEnumeration": "28",
                        }
                    ],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": "Fullständig beskrivning (OCR) ur SHBD: Larsson, Gunilla, Curt Weibull : en bibliografi 19 augusti 1976. -Stockholm, 1976. - 27 s. - (Acta Bibliothecae regiae Stockholmiensis, ISSN 0065-1060 ; 28)",
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/btm97svn1cccvps",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/2352800"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2004-01-21T10:22:13+01:00",
                    "mainEntity": {"@id": "https://libris.kb.se/btm97svn1cccvps#it"},
                    "marc:linked": {
                        "@id": "https://id.kb.se/marc/RecordHasLinks-Obsolete"
                    },
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9924807804"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "2352800",
                    "encodingLevel": "marc:MinimalLevel",
                    "generationDate": "2026-03-05T14:44:58.567+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/SHB"},
                    "marc:catalogingSource": {
                        "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                    },
                },
                {
                    "@id": "https://libris.kb.se/btm97svn1cccvps#it",
                    "part": ["1992 (58), s. [151]-156, 283"],
                    "@type": "PhysicalResource",
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/2352800"}],
                    "category": [
                        {"@id": "https://id.kb.se/term/saobf/Print"},
                        {"@id": "https://id.kb.se/term/saobf/ComponentPart"},
                    ],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "subtitle": "en anteckning",
                            "mainTitle": "Curt Weibull",
                        }
                    ],
                    "isPartOf": [{"@id": "https://libris.kb.se/j1tqppqv53g0tmr#it"}],
                    "instanceOf": {
                        "@type": "Monograph",
                        "hasNote": [
                            {
                                "@type": "marc:LanguageNote",
                                "label": ["Summary: Curt Weibull : a note"],
                            }
                        ],
                        "subject": [{"@id": "https://libris.kb.se/pm135z271l26txm#it"}],
                        "summary": [
                            {
                                "@type": "Summary",
                                "language": [{"@id": "https://id.kb.se/language/eng"}],
                            }
                        ],
                        "category": [
                            {
                                "@id": "https://id.kb.se/term/saogf/Biografier%20%C3%B6ver%20en%20individ"
                            },
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/swe"}],
                        "contribution": [
                            {
                                "@type": "PrimaryContribution",
                                "agent": {
                                    "@id": "https://libris.kb.se/jgvxzrh21bxdj1d#it"
                                },
                            }
                        ],
                        "classification": [
                            {
                                "code": "Lz Weibull, Curt",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F6/"}
                                    ],
                                    "version": "6",
                                },
                            }
                        ],
                    },
                    "publication": [
                        {
                            "year": "1992",
                            "@type": "PrimaryPublication",
                            "country": {"@id": "https://id.kb.se/country/sw"},
                        }
                    ],
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": False,
        "description": "Expected non-match.",
    },
]

# Cases below cannot be used to test the matching logic, as the Libris records are not actually part of the API search results. 
# They might instead be used to decide how to format the API queries.
API_SEACRH_TEST_CASES = [
    {
        "shb_rec": {
            "@graph": [
                {
                    "recordStatus": "marc:CorrectedOrRevised",
                    "encodingLevel": "marc:LessThanFullLevelMaterialNotExamined",
                    "descriptionConventions": [
                        {"@id": "https://id.kb.se/marc/CatFormType-a"}
                    ],
                    "modified": "2023-05-09T16:46:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/25256",
                    "@type": "Record",
                    "mainEntity": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/25256#it"
                    },
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/25256#2682",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/219595"},
                        "item": "2682",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/25256#2682"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/25256#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/1772-1809%20%28gustavianska%20tiden%2C%20Sverige%29"
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.455",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [{"@id": "https://id.kb.se/term/kssb/8"}],
                                    "version": "8",
                                },
                            }
                        ],
                    },
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Bref och uppteckningar från kriget i Finland 1808-1809",
                            "subtitle": "Utgifna af B. Hausen. x + + 1 portr. Skrifter utgivna av Svenska Litteratursällskapet i Finland",
                        }
                    ],
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": "Helsingfors"}],
                            "year": "1916",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": ["219 s."]}],
                    "seriesMembership": [
                        {
                            "@type": "SeriesMembership",
                            "inSeries": {
                                "@type": "PhysicalResource",
                                "category": [
                                    {"@id": "https://id.kb.se/term/saobf/Print"}
                                ],
                                "instanceOf": {
                                    "@type": "Series",
                                    "hasTitle": [
                                        {
                                            "@type": "Title",
                                            "mainTitle": "130. Innehåller: Kammarrådet F. L. Nybergs bref bok; Byttmästare C. M. Möllersvärds journal; Kapten H. Wärnhjelms relation om Sveaborgs kapitulation",
                                        }
                                    ],
                                },
                            },
                        }
                    ],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Bref och uppteckningar från kriget i Finland 1808-1809. Utgifna af B. Hausen. x + 219 s. + 1 portr. Helsingfors 1916. Skrifter utgivna av Svenska Litteratursällskapet i Finland. 130. Innehåller: Kammarrådet F. L. Nybergs bref bok; Byttmästare C. M. Möllersvärds journal; Kapten H. Wärnhjelms relation om Sveaborgs kapitulation."
                            ],
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/r82n7db32dpr3tq",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/386313"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2013-10-22T09:23:00+02:00",
                    "mainEntity": {"@id": "https://libris.kb.se/r82n7db32dpr3tq#it"},
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9903991029"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "386313",
                    "encodingLevel": "marc:AbbreviatedLevel",
                    "generationDate": "2026-03-04T16:33:03.98+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/H"},
                    "marc:catalogingSource": {
                        "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                    },
                },
                {
                    "@id": "https://libris.kb.se/r82n7db32dpr3tq#it",
                    "@type": "PhysicalResource",
                    "extent": [{"@type": "Extent", "label": ["219 s."]}],
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/386313"}],
                    "category": [
                        {"@id": "https://id.kb.se/term/saobf/Print"},
                        {"@id": "https://id.kb.se/term/rda/Volume"},
                    ],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Bref och uppteckningar från kriget i Finland 1808-1809",
                        }
                    ],
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [
                            {"@id": "https://id.kb.se/term/ktg/NonFictionLiterature"},
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/swe"}],
                        "contribution": [
                            {
                                "role": [{"@id": "https://id.kb.se/relator/editor"}],
                                "@type": "Contribution",
                                "agent": {
                                    "@id": "https://libris.kb.se/nl0245s60g9802v#it"
                                },
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.45(u)",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F5/"}
                                    ],
                                    "version": "5",
                                },
                            }
                        ],
                    },
                    "publication": [
                        {
                            "year": "1916",
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["Helsingfors"]}],
                            "country": {"@id": "https://id.kb.se/country/fi"},
                        }
                    ],
                    "seriesMembership": [
                        {
                            "@type": "SeriesMembership",
                            "inSeries": {
                                "@type": "Instance",
                                "instanceOf": {
                                    "@type": "Work",
                                    "hasTitle": [
                                        {
                                            "@type": "Title",
                                            "mainTitle": "Skrifter / utgivna av Svenska litteratursällskapet i Finland",
                                        }
                                    ],
                                },
                                "identifiedBy": [
                                    {"@type": "ISSN", "value": "0039-6842"}
                                ],
                            },
                            "seriesEnumeration": "130",
                        }
                    ],
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": True,
        "description": "Ger två träffar om man tar bort 'Sthlm', vilket saknas i Libris där man istället skrivit ut 'Stockholm'.",
    },
    {
        "shb_rec": {
            "@graph": [
                {
                    "recordStatus": "marc:CorrectedOrRevised",
                    "encodingLevel": "marc:LessThanFullLevelMaterialNotExamined",
                    "descriptionConventions": [
                        {"@id": "https://id.kb.se/marc/CatFormType-a"}
                    ],
                    "modified": "2023-05-09T16:46:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/25255",
                    "@type": "Record",
                    "mainEntity": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/25255#it"
                    },
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/25255#2681",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/219595"},
                        "item": "2681",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/25255#2681"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/25255#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/1772-1809%20%28gustavianska%20tiden%2C%20Sverige%29"
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.455",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [{"@id": "https://id.kb.se/term/kssb/8"}],
                                    "version": "8",
                                },
                            }
                        ],
                    },
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Finska kriget 1808 och 1809",
                            "subtitle": "Läsning för ung och gammal. 3:e öfversedda o. tillökade uppl",
                        }
                    ],
                    "responsibilityStatement": "Björlin, G.",
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": "Sthlm"}],
                            "year": "1906",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": ["6 pl."]}],
                    "seriesMembership": [
                        {
                            "@type": "SeriesMembership",
                            "inSeries": {
                                "@type": "PhysicalResource",
                                "category": [
                                    {"@id": "https://id.kb.se/term/saobf/Print"}
                                ],
                                "instanceOf": {
                                    "@type": "Series",
                                    "hasTitle": [
                                        {"@type": "Title", "mainTitle": "Med"}
                                    ],
                                },
                            },
                            "seriesEnumeration": "16 portr. o. 18 kartor. 357 + (3) s",
                        }
                    ],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Björlin, G., Finska kriget 1808 och 1809. Läsning för ung och gammal. 3:e öfversedda o. tillökade uppl. Med 16 portr., 6 pl. o. 18 kartor. 357 + (3) s. Sthlm 1906."
                            ],
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/vc5stwf61ttdw47",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/1612116"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2011-05-25T12:48:46+02:00",
                    "mainEntity": {"@id": "https://libris.kb.se/vc5stwf61ttdw47#it"},
                    "bibliography": [{"@id": "https://libris.kb.se/library/BOTHAC"}],
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9916808058"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "1612116",
                    "encodingLevel": "marc:AbbreviatedLevel",
                    "generationDate": "2026-03-05T15:48:43.817+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/NBR"},
                    "descriptionConventions": [{"@id": "https://id.kb.se/marc/Aacr2"}],
                },
                {
                    "@id": "https://libris.kb.se/vc5stwf61ttdw47#it",
                    "@type": "PhysicalResource",
                    "extent": [{"@type": "Extent", "label": ["360 s., 6 pl.-bl."]}],
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/1612116"}],
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "subtitle": "läsning för ung och gammal : med 16 porträtt, 6 planscher och 18 kartor",
                            "mainTitle": "Finska kriget 1808 och 1809",
                        }
                    ],
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [
                            {"@id": "https://id.kb.se/term/ktg/NonFictionLiterature"},
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/swe"}],
                        "contribution": [
                            {
                                "role": {"@id": "https://id.kb.se/relator/author"},
                                "@type": "PrimaryContribution",
                                "agent": {
                                    "@id": "https://libris.kb.se/0xbdd40j5mfs6lr#it"
                                },
                            }
                        ],
                        "classification": [
                            {"code": '355.49(485)"18"', "@type": "ClassificationUdc"},
                            {
                                "code": "S-c:k",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F6/"}
                                    ],
                                    "version": "6",
                                },
                            },
                        ],
                    },
                    "publication": [
                        {
                            "year": "1906",
                            "@type": "PrimaryPublication",
                            "agent": {"@type": "Agent", "label": ["Norstedt"]},
                            "place": [{"@type": "Place", "label": ["Stockholm"]}],
                            "country": {"@id": "https://id.kb.se/country/sw"},
                        }
                    ],
                    "editionStatement": "3. tillök. uppl.",
                    "physicalDetailsNote": "ill.",
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": True,
        "description": "Ger två träffar om man tar bort 'Sthlm', vilket saknas i Libris där man istället skrivit ut 'Stockholm'.",
    },
    {
        "shb_rec": {
            "@graph": [
                {
                    "recordStatus": "marc:CorrectedOrRevised",
                    "encodingLevel": "marc:LessThanFullLevelMaterialNotExamined",
                    "descriptionConventions": [
                        {"@id": "https://id.kb.se/marc/CatFormType-a"}
                    ],
                    "modified": "2023-05-09T16:46:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/25356",
                    "@type": "Record",
                    "mainEntity": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/25356#it"
                    },
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/25356#2782",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/219595"},
                        "item": "2782",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/25356#2782"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/25356#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/1772-1809%20%28gustavianska%20tiden%2C%20Sverige%29"
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.455",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [{"@id": "https://id.kb.se/term/kssb/8"}],
                                    "version": "8",
                                },
                            }
                        ],
                    },
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Freden i Fredrikshamn",
                            "subtitle": "Akad",
                        }
                    ],
                    "responsibilityStatement": "Hamnström, E.",
                    "extent": [{"@type": "Extent", "label": ["131 + (1) s."]}],
                    "seriesMembership": [
                        {
                            "@type": "SeriesMembership",
                            "inSeries": {
                                "@type": "PhysicalResource",
                                "category": [
                                    {"@id": "https://id.kb.se/term/saobf/Print"}
                                ],
                                "instanceOf": {
                                    "@type": "Series",
                                    "hasTitle": [
                                        {
                                            "@type": "Title",
                                            "mainTitle": "afh. vin + Uppsala",
                                        }
                                    ],
                                },
                            },
                            "seriesEnumeration": "1902",
                        }
                    ],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Hamnström, E., Freden i Fredrikshamn. Akad. afh. vin + 131 + (1) s. Uppsala 1902."
                            ],
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/5ng2kgxh1gf0nnc",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/350505"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2022-06-02T11:09:07.102+02:00",
                    "mainEntity": {"@id": "https://libris.kb.se/5ng2kgxh1gf0nnc#it"},
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9903589298"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "350505",
                    "encodingLevel": "marc:MinimalLevel",
                    "generationDate": "2026-03-05T12:57:01.405+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/Li"},
                    "marc:catalogingSource": {
                        "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                    },
                    "descriptionLastModifier": {
                        "@id": "https://libris.kb.se/library/Alb"
                    },
                },
                {
                    "@id": "https://libris.kb.se/5ng2kgxh1gf0nnc#it",
                    "@type": "PhysicalResource",
                    "extent": [{"@type": "Extent", "label": ["viii, 131 s."]}],
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/350505"}],
                    "category": [
                        {"@id": "https://id.kb.se/term/saobf/Print"},
                        {"@id": "https://id.kb.se/term/rda/Volume"},
                    ],
                    "hasTitle": [
                        {"@type": "Title", "mainTitle": "Freden i Fredrikshamn"}
                    ],
                    "instanceOf": {
                        "@type": "Monograph",
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/Finska%20kriget%201808-1809"
                            },
                            {
                                "@id": "https://id.kb.se/term/sao/Fredsf%C3%B6rhandlingar"
                            },
                        ],
                        "category": [
                            {"@id": "https://id.kb.se/term/saogf/Avhandlingar"},
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/swe"}],
                        "contribution": [
                            {
                                "role": {"@id": "https://id.kb.se/relator/author"},
                                "@type": "PrimaryContribution",
                                "agent": {
                                    "@type": "Person",
                                    "givenName": "Erik",
                                    "familyName": "Hamnström",
                                },
                            }
                        ],
                        "dissertation": [
                            {
                                "@type": "Dissertation",
                                "label": ["Diss. Uppsala : Univ."],
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.47",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F5/"}
                                    ],
                                    "version": "5",
                                },
                            }
                        ],
                    },
                    "publication": [
                        {
                            "year": "1902",
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["Upsala"]}],
                            "country": {"@id": "https://id.kb.se/country/sw"},
                        }
                    ],
                    "responsibilityStatement": "af Erik Hamnström",
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": True,
        "description": "matchar efter omflyttning/radering av text i nuvarande sökfråga",
    },
    {
        "shb_rec": {
            "@graph": [
                {
                    "recordStatus": "marc:CorrectedOrRevised",
                    "encodingLevel": "marc:LessThanFullLevelMaterialNotExamined",
                    "descriptionConventions": [
                        {"@id": "https://id.kb.se/marc/CatFormType-a"}
                    ],
                    "modified": "2023-05-09T16:46:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/24943",
                    "@type": "Record",
                    "mainEntity": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/24943#it"
                    },
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/24943#2369",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/219595"},
                        "item": "2369",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/24943#2369"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/24943#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/1718-1772%20%28frihetstiden%2C%20Sverige%29"
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.44",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [{"@id": "https://id.kb.se/term/kssb/8"}],
                                    "version": "8",
                                },
                            }
                        ],
                    },
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Ryska besöket i Norrköping 1719",
                            "subtitle": "Särtryck ur Norrköpings Tidningar",
                        }
                    ],
                    "responsibilityStatement": "Hallendorff, C",
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": "Norrköping"}],
                            "year": "1902",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": ["36 s."]}],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Hallendorff, C, Ryska besöket i Norrköping 1719. 36 s. Norrköping 1902. Särtryck ur Norrköpings Tidningar."
                            ],
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/q71p86x20jzp7x6",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/2056382"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2007-04-03T15:32:03+02:00",
                    "mainEntity": {"@id": "https://libris.kb.se/q71p86x20jzp7x6#it"},
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9921594443"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "2056382",
                    "encodingLevel": "marc:AbbreviatedLevel",
                    "generationDate": "2026-03-05T15:34:06.99+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/Ra"},
                    "marc:catalogingSource": {
                        "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                    },
                },
                {
                    "@id": "https://libris.kb.se/q71p86x20jzp7x6#it",
                    "@type": "PhysicalResource",
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/2056382"}],
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Ryska besöket i Norrköping 1719",
                        }
                    ],
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [
                            {"@id": "https://id.kb.se/term/ktg/NonFictionLiterature"},
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/swe"}],
                        "contribution": [
                            {
                                "role": {"@id": "https://id.kb.se/relator/author"},
                                "@type": "PrimaryContribution",
                                "agent": {
                                    "@id": "https://libris.kb.se/fcrvznzz2ml6bd9#it"
                                },
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.441",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F6/"}
                                    ],
                                    "version": "6",
                                },
                            },
                            {
                                "code": "Kma.43",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F6/"}
                                    ],
                                    "version": "6",
                                },
                            },
                        ],
                    },
                    "publication": [
                        {
                            "year": "1902",
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["Norrköping"]}],
                            "country": {"@id": "https://id.kb.se/country/sw"},
                        }
                    ],
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": True,
        "description": "Är förmodligen denna, men sidantal saknas i posten",
    },
]
