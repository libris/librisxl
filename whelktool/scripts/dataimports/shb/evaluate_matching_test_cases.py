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
        "description": "",
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
        "description": "",
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
        "expected_to_match": True,
        "description": "Fake example of how an unexpected non-match would be displayed.",
    },
]
