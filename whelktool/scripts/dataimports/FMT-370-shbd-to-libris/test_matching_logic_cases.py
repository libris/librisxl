TEMPLATE = [
    {
        "shb_rec": {},
        "libris_rec": {},
        "expected_to_match": True,
        "description": "",
    }
]

BUILDING = []


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
                    "@id": "https://libris-qa.kb.se/dataset/shb/597",
                    "@type": "Record",
                    "mainEntity": {"@id": "https://libris-qa.kb.se/dataset/shb/597#it"},
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/597#446",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/3685700"},
                        "item": "446",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/597#446"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/597#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/1611-1718%20%28stormaktstiden%2C%20Sverige%29"
                            },
                            {"@id": "https://id.kb.se/term/sao/Historia"},
                            {"@id": "https://id.kb.se/term/sao/Baltikum"},
                        ],
                        "classification": [
                            {
                                "code": "Kmc",
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
                            "mainTitle": "Livet på Runö",
                            "subtitle": "en berättelse om hur 300 människor levde på den lilla svenskön Runö i Rigaviken från 1920-talet fram till andra världskriget",
                        }
                    ],
                    "responsibilityStatement": "Steffensson, Jakob",
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["Stockholm"]}],
                            "year": "1976",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": ["174 s.: ill."]}],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Rec. i Expr 16.10.1976 av D. Tarschys; i VasterbF 21.10.1976 avM. Rosin; i BT 27.10.1976 av E.-K. Almerud; i Bohusl 5.11.1976 av 3. Gerd; i Vbl 18.11.1976 av I. H.; i Hbl 19.11.1976 av E. von Kraemer; i JP 2.12.1976 av I. Hesslander; i SDS 5.12.1976 av G. Irbe; i GP 7.12.1976 av S. U. Palme; i HD 30.12.1976 av B. Gunnemo; i SkD 14.1.1977 av A. Pall; i UNT 18.1.1977 av T. Derblom-Anderson; i Arbbl 25.7.1977 av S. Larsson"
                            ],
                        },
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Steffensson, Jakob, Livet på Runö : en berättelse om hur 300 människor levde på den lilla svenskön Runö i Rigaviken från 1920-talet fram till andra världskriget. - Stockholm, 1976. - 174 s.: ill. Rec. i Expr 16.10.1976 av D. Tarschys; i VasterbF 21.10.1976 avM. Rosin; i BT 27.10.1976 av E.-K. Almerud; i Bohusl 5.11.1976 av 3. Gerd; i Vbl 18.11.1976 av I. H.; i Hbl 19.11.1976 av E. von Kraemer; i JP 2.12.1976 av I. Hesslander; i SDS 5.12.1976 av G. Irbe; i GP 7.12.1976 av S. U. Palme; i HD 30.12.1976 av B. Gunnemo; i SkD 14.1.1977 av A. Pall; i UNT 18.1.1977 av T. Derblom-Anderson; i Arbbl 25.7.1977 av S. Larsson"
                            ],
                        },
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/vc51srt60h7z18n",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/7251876"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2023-01-12T07:24:46.241+01:00",
                    "mainEntity": {"@id": "https://libris.kb.se/vc51srt60h7z18n#it"},
                    "bibliography": [{"@id": "https://libris.kb.se/library/NB"}],
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9136008133"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "7251876",
                    "encodingLevel": "marc:FullLevel",
                    "generationDate": "2026-03-04T18:22:48.995+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/NB"},
                    "descriptionConventions": [{"@id": "https://id.kb.se/marc/Aacr2"}],
                    "descriptionLastModifier": {
                        "@id": "https://libris.kb.se/library/Smm"
                    },
                },
                {
                    "@id": "https://libris.kb.se/vc51srt60h7z18n#it",
                    "@type": "PhysicalResource",
                    "extent": [{"@type": "Extent", "label": ["174, [1] s."]}],
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/7251876"}],
                    "category": [
                        {"@id": "https://id.kb.se/term/saobf/Print"},
                        {"@id": "https://id.kb.se/term/rda/Volume"},
                    ],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "subtitle": "en berättelse om hur 300 människor levde på den lilla svenskön Runö i Rigaviken från 1920-talet fram till andra världskriget",
                            "mainTitle": "Livet på Runö",
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
                                    "@id": "https://libris.kb.se/tr5797tc3wg1g5m#it"
                                },
                            }
                        ],
                        "classification": [
                            {
                                "code": "Mcx-mca",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F5/"}
                                    ],
                                    "version": "5",
                                },
                            },
                            {
                                "code": "Nmcaz Runö",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F5/"}
                                    ],
                                    "version": "5",
                                },
                            },
                            {"code": "39(474.2)", "@type": "ClassificationUdc"},
                            {"code": "629.122", "@type": "ClassificationUdc"},
                            {"code": "639.13", "@type": "ClassificationUdc"},
                            {"code": "639.2(474.2)", "@type": "ClassificationUdc"},
                            {
                                "code": "305.839709",
                                "@type": "ClassificationDdc",
                                "edition": "full",
                                "editionEnumeration": "23/swe",
                            },
                        ],
                    },
                    "manufacture": [
                        {
                            "@type": "Manufacture",
                            "agent": {"@type": "Agent", "label": ["Centraltr."]},
                            "place": [{"@type": "Place", "label": ["Borås"]}],
                        }
                    ],
                    "publication": [
                        {
                            "year": "1976",
                            "@type": "PrimaryPublication",
                            "agent": {"@type": "Agent", "label": ["LT"]},
                            "place": [{"@type": "Place", "label": ["Stockholm"]}],
                            "country": {"@id": "https://id.kb.se/country/sw"},
                        }
                    ],
                    "identifiedBy": [
                        {
                            "@type": "ISBN",
                            "value": "9136008133",
                            "qualifier": ["INB."],
                            "acquisitionTerms": ["65:00"],
                        }
                    ],
                    "hasDimensions": {"@type": "Dimensions", "label": ["22 cm"]},
                    "physicalDetailsNote": "ill.",
                    "responsibilityStatement": "Jakob Steffensson",
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": True,
        "description": "Två träffar från API:t. Denna verkar vara en korrekt match.",
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
                    "@id": "https://libris-qa.kb.se/dataset/shb/597",
                    "@type": "Record",
                    "mainEntity": {"@id": "https://libris-qa.kb.se/dataset/shb/597#it"},
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/597#446",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/3685700"},
                        "item": "446",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/597#446"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/597#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/1611-1718%20%28stormaktstiden%2C%20Sverige%29"
                            },
                            {"@id": "https://id.kb.se/term/sao/Historia"},
                            {"@id": "https://id.kb.se/term/sao/Baltikum"},
                        ],
                        "classification": [
                            {
                                "code": "Kmc",
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
                            "mainTitle": "Livet på Runö",
                            "subtitle": "en berättelse om hur 300 människor levde på den lilla svenskön Runö i Rigaviken från 1920-talet fram till andra världskriget",
                        }
                    ],
                    "responsibilityStatement": "Steffensson, Jakob",
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["Stockholm"]}],
                            "year": "1976",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": ["174 s.: ill."]}],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Rec. i Expr 16.10.1976 av D. Tarschys; i VasterbF 21.10.1976 avM. Rosin; i BT 27.10.1976 av E.-K. Almerud; i Bohusl 5.11.1976 av 3. Gerd; i Vbl 18.11.1976 av I. H.; i Hbl 19.11.1976 av E. von Kraemer; i JP 2.12.1976 av I. Hesslander; i SDS 5.12.1976 av G. Irbe; i GP 7.12.1976 av S. U. Palme; i HD 30.12.1976 av B. Gunnemo; i SkD 14.1.1977 av A. Pall; i UNT 18.1.1977 av T. Derblom-Anderson; i Arbbl 25.7.1977 av S. Larsson"
                            ],
                        },
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Steffensson, Jakob, Livet på Runö : en berättelse om hur 300 människor levde på den lilla svenskön Runö i Rigaviken från 1920-talet fram till andra världskriget. - Stockholm, 1976. - 174 s.: ill. Rec. i Expr 16.10.1976 av D. Tarschys; i VasterbF 21.10.1976 avM. Rosin; i BT 27.10.1976 av E.-K. Almerud; i Bohusl 5.11.1976 av 3. Gerd; i Vbl 18.11.1976 av I. H.; i Hbl 19.11.1976 av E. von Kraemer; i JP 2.12.1976 av I. Hesslander; i SDS 5.12.1976 av G. Irbe; i GP 7.12.1976 av S. U. Palme; i HD 30.12.1976 av B. Gunnemo; i SkD 14.1.1977 av A. Pall; i UNT 18.1.1977 av T. Derblom-Anderson; i Arbbl 25.7.1977 av S. Larsson"
                            ],
                        },
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/9tmpszbm1pttcmg",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/12564649"}],
                    "created": "2012-02-27T15:27:48+01:00",
                    "modified": "2025-09-26T19:07:42.16+02:00",
                    "mainEntity": {"@id": "https://libris.kb.se/9tmpszbm1pttcmg#it"},
                    "bibliography": [{"@id": "https://libris.kb.se/library/Mtm"}],
                    "identifiedBy": [{"@type": "SystemNumber", "value": "(MTM)C60225"}],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "12564649",
                    "encodingLevel": "marc:MinimalLevel",
                    "technicalNote": [
                        {
                            "@type": "TechnicalNote",
                            "label": ["MTM MarcRecordId: 139590"],
                        }
                    ],
                    "generationDate": "2026-03-05T08:58:58.051+01:00",
                    "_marcUncompleted": {
                        "856": {
                            "ind1": " ",
                            "ind2": " ",
                            "subfields": {"s": "127248113"},
                        },
                        "_unhandled": "s",
                    },
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/me/devops/globalchanges-PROD/librisxl/whelktool/reports/prod-20260305-082639/restore/restore.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/Mtm"},
                    "marc:catalogingSource": {
                        "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                    },
                    "descriptionConventions": [{"@id": "https://id.kb.se/marc/Aacr2"}],
                    "descriptionLastModifier": {
                        "@id": "https://libris.kb.se/library/Mtm"
                    },
                },
                {
                    "@id": "https://libris.kb.se/9tmpszbm1pttcmg#it",
                    "@type": "PhysicalResource",
                    "extent": [
                        {"@type": "Extent", "label": ["1 CD-R (5 tim., 52 min.)"]}
                    ],
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/12564649"}],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": ["Digital talbok (DAISY 2.02), ljud"],
                        },
                        {
                            "@type": "Note",
                            "label": [
                                "Överföring av analog inläsning från: Enskede : TPB, 1985"
                            ],
                        },
                        {"@type": "Note", "label": ["Inläst ur: Stockholm : LT, 1976"]},
                    ],
                    "summary": [
                        {
                            "@type": "Summary",
                            "label": [
                                "Den f. d. svenskön Runö ligger i Rigabukten, 65 km från estländska fastlandet och 230 km från Gotland. Runö är speciellt intressant p. g. a. likheten med Sverige i fråga om seder och bruk, om än ålderdomligare än de levnadsförhållanden vi varit vana vid under motsvarande tid. Förf. skildrar 1920-talet fram till 2:a världskriget, den tid då ca 300 personer levde på ön, de flesta som deltidsjordbrukare med jakt och fiske som viktigaste sysselsättning"
                            ],
                        }
                    ],
                    "category": [{"@id": "https://id.kb.se/term/rda/AudioDisc"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Livet på Runö",
                            "titleRemainder": "en berättelse om hur 300 människor levde på den lilla svenskön Runö i Rigaviken från 1920-talet fram till andra världskriget",
                        }
                    ],
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [
                            {"@id": "https://id.kb.se/term/saogf/Talb%C3%B6cker"},
                            {"@id": "https://id.kb.se/term/ktg/Audio"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/swe"}],
                        "contribution": [
                            {
                                "@type": "PrimaryContribution",
                                "agent": {
                                    "@type": "Person",
                                    "givenName": "Jakob",
                                    "familyName": "Steffensson",
                                },
                            },
                            {
                                "role": [{"@id": "https://id.kb.se/relator/narrator"}],
                                "@type": "Contribution",
                                "agent": {
                                    "@type": "Person",
                                    "givenName": "Bo",
                                    "familyName": "Isaksson",
                                },
                            },
                        ],
                        "classification": [
                            {
                                "code": "Kcx-mcaz Runö",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F8/"}
                                    ],
                                    "version": "8",
                                },
                            },
                            {
                                "code": "Nmcaz Runö",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F8/"}
                                    ],
                                    "version": "8",
                                },
                            },
                            {
                                "code": "Mcx-mcaz Runö",
                                "@type": "Classification",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F8/"}
                                    ],
                                    "version": "8",
                                },
                            },
                        ],
                        "intendedAudience": [{"@id": "https://id.kb.se/marc/Adult"}],
                    },
                    "manufacture": [{"date": "[2012-02-27]", "@type": "Manufacture"}],
                    "publication": [
                        {
                            "year": "2007",
                            "@type": "PrimaryPublication",
                            "agent": {"@type": "Agent", "label": ["TPB"]},
                            "place": [{"@type": "Place", "label": ["Enskede"]}],
                            "country": {"@id": "https://id.kb.se/country/sw"},
                        }
                    ],
                    "identifiedBy": [
                        {
                            "@type": "Identifier",
                            "value": "C60225",
                            "typeNote": "MTM medienummer",
                            "qualifier": ["talbok", "bs"],
                        }
                    ],
                    "classification": [
                        {
                            "code": "Kcx-mcaz Runö/TC",
                            "@type": "Classification",
                            "inScheme": {
                                "code": "kssb",
                                "@type": "ConceptScheme",
                                "sameAs": [{"@id": "https://id.kb.se/term/kssb%2F8/"}],
                                "version": "8",
                            },
                        },
                        {
                            "code": "Nmcaz Runö/TC",
                            "@type": "Classification",
                            "inScheme": {
                                "code": "kssb",
                                "@type": "ConceptScheme",
                                "sameAs": [{"@id": "https://id.kb.se/term/kssb%2F8/"}],
                                "version": "8",
                            },
                        },
                        {
                            "code": "Mcx-mcaz Runö/TC",
                            "@type": "Classification",
                            "inScheme": {
                                "code": "kssb",
                                "@type": "ConceptScheme",
                                "sameAs": [{"@id": "https://id.kb.se/term/kssb%2F8/"}],
                                "version": "8",
                            },
                        },
                    ],
                    "electronicLocator": [{"@type": "Document"}],
                    "otherPhysicalFormat": [
                        {
                            "@type": "Instance",
                            "hasTitle": [
                                {"@type": "Title", "mainTitle": "Livet på Runö"}
                            ],
                            "marc:displayText": "Inläst ur",
                        }
                    ],
                    "physicalDetailsNote": "mono",
                    "usageAndAccessPolicy": [
                        {"@id": "https://id.kb.se/policy/mtm/restricted"}
                    ],
                    "digitalCharacteristic": [
                        {"@type": "FileSize", "label": ["122 MB"]},
                        {"@type": "EncodingFormat", "label": ["DAISY"]},
                    ],
                    "responsibilityStatement": "Jakob Steffensson",
                    "marc:hasSpecialCodedDates": {
                        "@type": "marc:SpecialCodedDates",
                        "marc:endOfDateValid": "20000301",
                        "marc:typeOfDateCode": "m",
                        "marc:beginningOfDateValid": "20000101",
                    },
                    "marc:hasForeignMARCInformationField": {
                        "@type": "marc:ForeignMARCInformationField",
                        "marc:partList": [
                            {"marc:foreignMarcSubfield-2": "BURK IV"},
                            {"marc:foreignMarcSubfield-b": "KICASWSWP021SWES BD"},
                        ],
                        "marc:foreignMarcSubfield-i1": "0",
                    },
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": False,
        "description": "Två träffar från API:t. Denna är en MTM-talbok.",
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
                    "@id": "https://libris-qa.kb.se/dataset/shb/1047",
                    "@type": "Record",
                    "mainEntity": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/1047#it"
                    },
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/1047#896",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/3685700"},
                        "item": "896",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/1047#896"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/1047#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {"@id": "https://id.kb.se/term/sao/Sverige"},
                            {"@id": "https://id.kb.se/term/sao/Biografier"},
                        ],
                        "classification": [
                            {
                                "code": "Lz",
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
                        {"@type": "Title", "mainTitle": "Blad ur minnenas bok"}
                    ],
                    "responsibilityStatement": "Eriksson, Karl",
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["[Alingsås]"]}],
                            "year": "1976",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": ["103 s. : ill."]}],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Eriksson, Karl, Blad ur minnenas bok. - [Alingsås], 1976. - 103 s. : ill."
                            ],
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/h0scn72t19058gj",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/146035"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2018-06-04T10:27:20.929+02:00",
                    "mainEntity": {"@id": "https://libris.kb.se/h0scn72t19058gj#it"},
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9901376696"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "146035",
                    "encodingLevel": "marc:FullLevel",
                    "generationDate": "2026-03-05T15:52:16.747+01:00",
                    "_marcUncompleted": {
                        "270": {
                            "ind1": " ",
                            "ind2": " ",
                            "subfields": {"a": "[Ödenäs]"},
                        }
                    },
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/NB"},
                    "descriptionConventions": [{"@id": "https://id.kb.se/marc/Aacr2"}],
                },
                {
                    "@id": "https://libris.kb.se/h0scn72t19058gj#it",
                    "@type": "PhysicalResource",
                    "extent": [{"@type": "Extent", "label": ["103 s."]}],
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/146035"}],
                    "category": [
                        {"@id": "https://id.kb.se/term/saobf/Print"},
                        {"@id": "https://id.kb.se/term/rda/Volume"},
                    ],
                    "hasTitle": [
                        {"@type": "Title", "mainTitle": "Blad ur minnenas bok"}
                    ],
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [
                            {"@id": "https://id.kb.se/term/saogf/Sj%C3%A4lvbiografier"},
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/swe"}],
                        "contribution": [
                            {
                                "role": {"@id": "https://id.kb.se/relator/author"},
                                "@type": "PrimaryContribution",
                                "agent": {
                                    "@type": "Person",
                                    "lifeSpan": "1895-",
                                    "givenName": "Karl",
                                    "familyName": "Eriksson",
                                },
                            }
                        ],
                        "classification": [
                            {
                                "code": "Lz Eriksson,K.",
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
                            "agent": {
                                "@type": "Agent",
                                "label": ["Västergötlands tr."],
                            },
                            "place": [{"@type": "Place", "label": ["Skara"]}],
                        }
                    ],
                    "publication": [
                        {
                            "year": "1976",
                            "@type": "PrimaryPublication",
                            "agent": {"@type": "Agent", "label": ["[Förf.]"]},
                            "place": [{"@type": "Place", "label": ["[Alingsås]"]}],
                            "country": {"@id": "https://id.kb.se/country/sw"},
                        }
                    ],
                    "identifiedBy": [{"@type": "ISBN", "acquisitionTerms": ["25:00"]}],
                    "hasDimensions": {"@type": "Dimensions", "label": ["21 cm"]},
                    "physicalDetailsNote": "ill.",
                    "responsibilityStatement": "Karl Eriksson",
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": True,
        "description": "En träff från API:t, ser ut att vara den korrekta.",
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
                    "modified": "2023-05-09T16:45:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/16499",
                    "@type": "Record",
                    "mainEntity": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/16499#it"
                    },
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/16499#6303",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/8208408"},
                        "item": "6303",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/16499#6303"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/16499#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {"@id": "https://id.kb.se/term/sao/Sverige"},
                            {"@id": "https://id.kb.se/term/sao/Kulturhistoria"},
                        ],
                        "classification": [
                            {
                                "code": "Kt-c",
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
                            "mainTitle": "Reise durch Schweden im Jahr 1804",
                        }
                    ],
                    "responsibilityStatement": "Arndt, E. M.",
                    "publication": [
                        {
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["Berlin"]}],
                            "year": "1806",
                        }
                    ],
                    "extent": [{"@type": "Extent", "label": ["Th. 1-4."]}],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Rec. i Göttingische gelehrte Anzeigen 1807. Bd 1, s. 673-680. Th. 1-2 rec. i Neueste critische Nachrichten 1806. Bd 32, s. 161-165"
                            ],
                        },
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Arndt, E. M., Reise durch Schweden im Jahr 1804. Th. 1-4. Berlin 1806. Rec. i Göttingische gelehrte Anzeigen 1807. Bd 1, s. 673-680. Th. 1-2 rec. i Neueste critische Nachrichten 1806. Bd 32, s. 161-165."
                            ],
                        },
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/6ph58wrj0f2n7zk",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/2494126"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2007-06-13T14:28:07+02:00",
                    "mainEntity": {"@id": "https://libris.kb.se/6ph58wrj0f2n7zk#it"},
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9926271073"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "2494126",
                    "encodingLevel": "marc:AbbreviatedLevel",
                    "technicalNote": [
                        {
                            "@type": "TechnicalNote",
                            "label": ["Huvudpost (flerbandsverk)"],
                        }
                    ],
                    "generationDate": "2026-03-05T13:07:39.624+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/ARJ"},
                    "marc:catalogingSource": {
                        "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                    },
                },
                {
                    "extent": [
                        {
                            "@type": "Extent",
                            "label": ["4 vol."],
                            "computedLabel": "4 vol.",
                        }
                    ],
                    "@type": "PhysicalResource",
                    "meta": {
                        "descriptionCreator": {
                            "@id": "https://libris.kb.se/library/ARJ"
                        },
                        "mainEntity": {
                            "@id": "https://libris.kb.se/6ph58wrj0f2n7zk#it"
                        },
                        "marc:catalogingSource": {
                            "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                        },
                        "encodingLevel": "marc:AbbreviatedLevel",
                        "@type": "Record",
                        "created": "2001-12-11T00:00:00+01:00",
                        "generationDate": "2026-03-05T13:07:39.624+01:00",
                        "technicalNote": [
                            {
                                "@type": "TechnicalNote",
                                "label": ["Huvudpost (flerbandsverk)"],
                                "computedLabel": "Huvudpost (flerbandsverk)",
                            }
                        ],
                        "recordStatus": "marc:CorrectedOrRevised",
                        "modified": "2007-06-13T14:28:07+02:00",
                        "@id": "https://libris.kb.se/6ph58wrj0f2n7zk",
                        "librissearch:controlNumbers": ["2494126", "6ph58wrj0f2n7zk"],
                        "identifiedBy": [
                            {
                                "@type": "LibrisIIINumber",
                                "value": "9926271073",
                                "computedLabel": "LIBRISIII-nummer 9926271073",
                            }
                        ],
                        "controlNumber": "2494126",
                        "generationProcess": {
                            "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                        },
                        "sameAs": [{"@id": "http://libris.kb.se/bib/2494126"}],
                        "computedLabel": "2494126 · Miniminivå",
                    },
                    "publication": [
                        {
                            "country": {
                                "code": "gw",
                                "@type": "Country",
                                "meta": {
                                    "mainEntity": {
                                        "@id": "https://id.kb.se/country/gw"
                                    },
                                    "@type": "Record",
                                    "@id": "https://libris.kb.se/41ljjkq7617xprg1",
                                    "computedLabel": "https://libris.kb.se/41ljjkq7617xprg1",
                                },
                                "@id": "https://id.kb.se/country/gw",
                                "prefLabelByLang": {"sv": "Tyskland", "en": "Germany"},
                                "computedLabel": "Tyskland",
                            },
                            "year": "1806",
                            "@type": "PrimaryPublication",
                            "librissearch:year": "1806",
                            "place": [
                                {
                                    "@type": "Place",
                                    "label": ["Berlin"],
                                    "computedLabel": "Berlin",
                                }
                            ],
                            "computedLabel": "Tyskland · Berlin, 1806",
                        }
                    ],
                    "_categoryByCollection": {
                        "@none": [
                            {
                                "inScheme": {"@id": "https://id.kb.se/term/saobf"},
                                "@type": "CarrierForm",
                                "meta": {
                                    "mainEntity": {
                                        "@id": "https://id.kb.se/term/saobf/Print"
                                    },
                                    "@type": "Record",
                                    "@id": "https://libris.kb.se/jq0mh5c5l2ff7sc2",
                                    "computedLabel": "https://libris.kb.se/jq0mh5c5l2ff7sc2",
                                },
                                "@id": "https://id.kb.se/term/saobf/Print",
                                "prefLabelByLang": {"sv": "Tryck", "en": "Print"},
                                "computedLabel": "Tryck · Bärarform · https://id.kb.se/term/saobf",
                            },
                            {
                                "code": "nc",
                                "inScheme": {"@id": "https://id.kb.se/term/rda"},
                                "@type": "CarrierType",
                                "meta": {
                                    "mainEntity": {
                                        "@id": "https://id.kb.se/term/rda/Volume"
                                    },
                                    "@type": "Record",
                                    "@id": "https://libris.kb.se/bq2n963md2b6xhs1",
                                    "computedLabel": "https://libris.kb.se/bq2n963md2b6xhs1",
                                },
                                "searchLabelByLang": {"sv": "Bok", "en": "Book"},
                                "inCollection": {
                                    "@id": "https://id.kb.se/term/div/select"
                                },
                                "@id": "https://id.kb.se/term/rda/Volume",
                                "prefLabelByLang": {"sv": "Volym", "en": "Volume"},
                                "sameAs": [
                                    {"@id": "https://id.kb.se/term/rda/carrier/nc"},
                                    {"@id": "https://id.kb.se/term/rda/carrier/volume"},
                                ],
                                "labelByLang": {"sv": "Volym", "en": "Volume"},
                                "computedLabel": "Volym · Bärartyp · https://id.kb.se/term/rda",
                            },
                        ],
                        "@type": None,
                    },
                    "reverseLinks": {
                        "totalItemsByRelation": {"itemOf": 10, "itemOf.instanceOf": 0},
                        "totalItems": 10,
                        "@type": "PartialCollectionView",
                        "@id": "https://libris.kb.se/find?o=https%3A%2F%2Flibris.kb.se%2F6ph58wrj0f2n7zk%23it",
                        "computedLabel": "https://libris.kb.se/find?o=https%3A%2F%2Flibris.kb.se%2F6ph58wrj0f2n7zk%23it",
                    },
                    "@id": "https://libris.kb.se/6ph58wrj0f2n7zk#it",
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "mainTitle": "Reise durch Schweden im Jahr 1804",
                            "computedLabel": "Reise durch Schweden im Jahr 1804",
                        }
                    ],
                    "@reverse": {
                        "itemOf": [
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/Osz",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/Os"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/7ckgnvxd4x7jb0d#it",
                                "computedLabel": "https://libris.kb.se/library/Osz",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/O",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/OUB"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/6bjgk7mc381889t#it",
                                "computedLabel": "https://libris.kb.se/library/O",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/G",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/GUB"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/sf2b44mnqp57q8z5#it",
                                "computedLabel": "https://libris.kb.se/library/G",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/S",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/KB"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/59gzvtqb21940pp#it",
                                "computedLabel": "https://libris.kb.se/library/S",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/E",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/E"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/37dwsrn81sk3sv4#it",
                                "computedLabel": "https://libris.kb.se/library/E",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/Sa",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/SA"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/15b222l63x04xt3#it",
                                "computedLabel": "https://libris.kb.se/library/Sa",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/SRo",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/KB"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/26dmxbn726mkpl6#it",
                                "computedLabel": "https://libris.kb.se/library/SRo",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/Vasb",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/VASB"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/lqx4rl7r1p442jl#it",
                                "computedLabel": "https://libris.kb.se/library/Vasb",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/Kclb",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/KKA"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/6bj0hx0c1h8c6qx#it",
                                "computedLabel": "https://libris.kb.se/library/Kclb",
                            },
                            {
                                "heldBy": {
                                    "@id": "https://libris.kb.se/library/L",
                                    "isPartOf": {
                                        "@id": "https://libris.kb.se/library/org/LUB"
                                    },
                                },
                                "@type": "Item",
                                "@id": "https://libris.kb.se/48fxtsp918nzv22#it",
                                "computedLabel": "https://libris.kb.se/library/L",
                            },
                        ]
                    },
                    "category": [
                        {
                            "inScheme": {"@id": "https://id.kb.se/term/saobf"},
                            "@type": "CarrierForm",
                            "meta": {
                                "mainEntity": {
                                    "@id": "https://id.kb.se/term/saobf/Print"
                                },
                                "@type": "Record",
                                "@id": "https://libris.kb.se/jq0mh5c5l2ff7sc2",
                                "computedLabel": "https://libris.kb.se/jq0mh5c5l2ff7sc2",
                            },
                            "@id": "https://id.kb.se/term/saobf/Print",
                            "prefLabelByLang": {"sv": "Tryck", "en": "Print"},
                            "computedLabel": "Tryck · Bärarform · https://id.kb.se/term/saobf",
                        },
                        {
                            "code": "nc",
                            "inScheme": {"@id": "https://id.kb.se/term/rda"},
                            "@type": "CarrierType",
                            "meta": {
                                "mainEntity": {
                                    "@id": "https://id.kb.se/term/rda/Volume"
                                },
                                "@type": "Record",
                                "@id": "https://libris.kb.se/bq2n963md2b6xhs1",
                                "computedLabel": "https://libris.kb.se/bq2n963md2b6xhs1",
                            },
                            "searchLabelByLang": {"sv": "Bok", "en": "Book"},
                            "inCollection": {"@id": "https://id.kb.se/term/div/select"},
                            "@id": "https://id.kb.se/term/rda/Volume",
                            "prefLabelByLang": {"sv": "Volym", "en": "Volume"},
                            "sameAs": [
                                {"@id": "https://id.kb.se/term/rda/carrier/nc"},
                                {"@id": "https://id.kb.se/term/rda/carrier/volume"},
                            ],
                            "labelByLang": {"sv": "Volym", "en": "Volume"},
                            "computedLabel": "Volym · Bärartyp · https://id.kb.se/term/rda",
                        },
                    ],
                    "instanceOf": {
                        "contribution": [
                            {
                                "agent": {
                                    "lifeSpan": "1769-1860",
                                    "@type": "Person",
                                    "meta": {
                                        "mainEntity": {
                                            "@id": "https://libris.kb.se/khw03pr31gn509c#it"
                                        },
                                        "@type": "Record",
                                        "@id": "https://libris.kb.se/khw03pr31gn509c",
                                        "computedLabel": "https://libris.kb.se/khw03pr31gn509c",
                                    },
                                    "familyName": "Arndt",
                                    "givenName": "Ernst Moritz",
                                    "@id": "https://libris.kb.se/khw03pr31gn509c#it",
                                    "hasVariant": [
                                        {
                                            "lifeSpan": "1769-1860",
                                            "@type": "Person",
                                            "familyName": "Arndt",
                                            "givenName": "E. M.",
                                            "fullerFormOfName": "(Ernst Moritz)",
                                            "computedLabel": "Arndt, E. M., 1769-1860, (Ernst Moritz)",
                                        }
                                    ],
                                    "sameAs": [
                                        {
                                            "@id": "http://libris.kb.se/resource/auth/286803"
                                        }
                                    ],
                                    "computedLabel": "Arndt, Ernst Moritz, 1769-1860",
                                },
                                "role": {
                                    "code": "aut",
                                    "@type": "Role",
                                    "meta": {
                                        "mainEntity": {
                                            "@id": "https://id.kb.se/relator/author"
                                        },
                                        "@type": "Record",
                                        "@id": "https://libris.kb.se/01671gl323zxzzhh",
                                        "computedLabel": "https://libris.kb.se/01671gl323zxzzhh",
                                    },
                                    "@id": "https://id.kb.se/relator/author",
                                    "prefLabelByLang": {
                                        "sv": "Författare",
                                        "en": "Author",
                                    },
                                    "sameAs": [{"@id": "https://id.kb.se/relator/aut"}],
                                    "computedLabel": "Författare",
                                },
                                "@type": "PrimaryContribution",
                                "computedLabel": "Arndt, Ernst Moritz, 1769-1860 (Författare)",
                            }
                        ],
                        "@type": "Monograph",
                        "language": [
                            {
                                "code": "ger",
                                "@type": "Language",
                                "meta": {
                                    "mainEntity": {
                                        "@id": "https://id.kb.se/language/ger"
                                    },
                                    "@type": "Record",
                                    "@id": "https://libris.kb.se/q8lc1l1vs3b7168t",
                                    "computedLabel": "https://libris.kb.se/q8lc1l1vs3b7168t",
                                },
                                "@id": "https://id.kb.se/language/ger",
                                "prefLabelByLang": {"sv": "Tyska", "en": "German"},
                                "sameAs": [{"@id": "https://id.kb.se/i18n/lang/de"}],
                                "computedLabel": "Tyska",
                            }
                        ],
                        "_categoryByCollection": {
                            "@none": [
                                {
                                    "@type": "GenreForm",
                                    "meta": {
                                        "mainEntity": {
                                            "@id": "https://id.kb.se/term/ktg/NonFictionLiterature"
                                        },
                                        "@type": "Record",
                                        "@id": "https://libris.kb.se/wjfqxkqjz3vsssck",
                                        "computedLabel": "https://libris.kb.se/wjfqxkqjz3vsssck",
                                    },
                                    "@id": "https://id.kb.se/term/ktg/NonFictionLiterature",
                                    "prefLabelByLang": {
                                        "sv": "Ej skönlitteratur",
                                        "en": "Non-fiction literature",
                                    },
                                    "computedLabel": "Ej skönlitteratur · Genre/form",
                                },
                                {
                                    "code": "txt",
                                    "inScheme": {"@id": "https://id.kb.se/term/rda"},
                                    "@type": "ContentType",
                                    "meta": {
                                        "mainEntity": {
                                            "@id": "https://id.kb.se/term/rda/Text"
                                        },
                                        "@type": "Record",
                                        "@id": "https://libris.kb.se/pnpsnkg0r5b6x092",
                                        "computedLabel": "https://libris.kb.se/pnpsnkg0r5b6x092",
                                    },
                                    "@id": "https://id.kb.se/term/rda/Text",
                                    "prefLabelByLang": {"sv": "Text", "en": "Text"},
                                    "sameAs": [
                                        {
                                            "@id": "https://id.kb.se/term/rda/content/text"
                                        },
                                        {
                                            "@id": "https://id.kb.se/term/rda/content/txt"
                                        },
                                    ],
                                    "computedLabel": "Text · Innehållstyp · https://id.kb.se/term/rda",
                                },
                            ],
                            "@type": None,
                        },
                        "category": [
                            {
                                "@type": "GenreForm",
                                "meta": {
                                    "mainEntity": {
                                        "@id": "https://id.kb.se/term/ktg/NonFictionLiterature"
                                    },
                                    "@type": "Record",
                                    "@id": "https://libris.kb.se/wjfqxkqjz3vsssck",
                                    "computedLabel": "https://libris.kb.se/wjfqxkqjz3vsssck",
                                },
                                "@id": "https://id.kb.se/term/ktg/NonFictionLiterature",
                                "prefLabelByLang": {
                                    "sv": "Ej skönlitteratur",
                                    "en": "Non-fiction literature",
                                },
                                "computedLabel": "Ej skönlitteratur · Genre/form",
                            },
                            {
                                "code": "txt",
                                "inScheme": {"@id": "https://id.kb.se/term/rda"},
                                "@type": "ContentType",
                                "meta": {
                                    "mainEntity": {
                                        "@id": "https://id.kb.se/term/rda/Text"
                                    },
                                    "@type": "Record",
                                    "@id": "https://libris.kb.se/pnpsnkg0r5b6x092",
                                    "computedLabel": "https://libris.kb.se/pnpsnkg0r5b6x092",
                                },
                                "@id": "https://id.kb.se/term/rda/Text",
                                "prefLabelByLang": {"sv": "Text", "en": "Text"},
                                "sameAs": [
                                    {"@id": "https://id.kb.se/term/rda/content/text"},
                                    {"@id": "https://id.kb.se/term/rda/content/txt"},
                                ],
                                "computedLabel": "Text · Innehållstyp · https://id.kb.se/term/rda",
                            },
                        ],
                        "classification": [
                            {
                                "code": "Nc.07",
                                "inScheme": {
                                    "code": "kssb",
                                    "@type": "ConceptScheme",
                                    "version": "5",
                                    "sameAs": [
                                        {"@id": "https://id.kb.se/term/kssb%2F5/"}
                                    ],
                                    "computedLabel": "kssb",
                                },
                                "@type": "Classification",
                                "_sab": "Nc.07",
                                "computedLabel": "Nc.07 · kssb · 5",
                            }
                        ],
                        "computedLabel": "Tyska · Arndt, Ernst Moritz, 1769-1860",
                    },
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/2494126"}],
                    "computedLabel": "Reise durch Schweden im Jahr 1804 · 1806",
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
                    "modified": "2023-05-09T16:48:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/32900",
                    "@type": "Record",
                    "mainEntity": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/32900#it"
                    },
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/32900#1796",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/8201921"},
                        "item": "1796",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/32900#1796"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/32900#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/1654-1718%20%28karolinska%20tiden%2C%20Sverige%29"
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.428",
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
                    "category": [
                        {"@id": "https://id.kb.se/term/saobf/Print"},
                        {"@id": "https://id.kb.se/term/saobf/ComponentPart"},
                    ],
                    "hasTitle": [{"@type": "Title", "mainTitle": "Karl XII"}],
                    "responsibilityStatement": "Bengtsson, Frans G.",
                    "isPartOf": [
                        {
                            "@type": "PhysicalResource",
                            "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                            "hasTitle": [{"@type": "Title", "mainTitle": "OoB"}],
                        }
                    ],
                    "part": ["1927, s. 1-10"],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Bengtsson, Frans G., Karl XII. (OoB 1927, s. 1-10.)"
                            ],
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/btmb04pn0gj0mw0",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/2982650"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2003-02-17T11:40:10+01:00",
                    "mainEntity": {"@id": "https://libris.kb.se/btmb04pn0gj0mw0#it"},
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9931291400"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "2982650",
                    "encodingLevel": "marc:AbbreviatedLevel",
                    "generationDate": "2026-03-05T12:45:53.549+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/ARJ"},
                    "marc:catalogingSource": {
                        "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                    },
                },
                {
                    "@id": "https://libris.kb.se/btmb04pn0gj0mw0#it",
                    "@type": "PhysicalResource",
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/2982650"}],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": ["Ur: Ord och bild. Årg. 36 (1927): s. 1-10."],
                        }
                    ],
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [{"@type": "Title", "mainTitle": "Karl XII"}],
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [
                            {"@id": "https://id.kb.se/term/ktg/NonFictionLiterature"},
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/und"}],
                        "contribution": [
                            {
                                "agent": {
                                    "lifeSpan": "1894-1954",
                                    "@type": "Person",
                                    "familyName": "Bengtsson",
                                    "givenName": "Frans G.",
                                    "@id": "https://libris.kb.se/b8nqqfvv43djcm1#it",
                                    "computedLabel": "Bengtsson, Frans G., 1894-1954",
                                },
                                "@type": "PrimaryContribution",
                                "computedLabel": "Bengtsson, Frans G., 1894-1954 (Författare)",
                            }
                        ],
                    },
                    "publication": [
                        {
                            "date": "<S.a.>",
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["<S.l.>"]}],
                        }
                    ],
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": True,
        "description": "6 träffar från API. Denna verkar vara en matchning, men faller på inkonsekvent angiven serieuppgift i Libris och SHB.",
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
                    "modified": "2023-05-09T16:48:00.0+02:00",
                    "bibliography": [{"@id": "https://libris.kb.se/library/SHB"}],
                    "@id": "https://libris-qa.kb.se/dataset/shb/32900",
                    "@type": "Record",
                    "mainEntity": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/32900#it"
                    },
                    "describes": {
                        "@id": "https://libris-qa.kb.se/dataset/shb/32900#1796",
                        "@type": "Source",
                        "isPartOf": {"@id": "http://libris.kb.se/resource/bib/8201921"},
                        "item": "1796",
                    },
                    "_statementBy": {
                        "bibliography": {
                            "source": {
                                "@id": "https://libris-qa.kb.se/dataset/shb/32900#1796"
                            },
                            "_object": {"@id": "https://libris.kb.se/library/SHB"},
                        }
                    },
                },
                {
                    "@id": "https://libris-qa.kb.se/dataset/shb/32900#it",
                    "@type": "PhysicalResource",
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [{"@id": "https://id.kb.se/term/rda/Text"}],
                        "subject": [
                            {
                                "@id": "https://id.kb.se/term/sao/1654-1718%20%28karolinska%20tiden%2C%20Sverige%29"
                            }
                        ],
                        "classification": [
                            {
                                "code": "Kc.428",
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
                    "category": [
                        {"@id": "https://id.kb.se/term/saobf/Print"},
                        {"@id": "https://id.kb.se/term/saobf/ComponentPart"},
                    ],
                    "hasTitle": [{"@type": "Title", "mainTitle": "Karl XII"}],
                    "responsibilityStatement": "Bengtsson, Frans G.",
                    "isPartOf": [
                        {
                            "@type": "PhysicalResource",
                            "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                            "hasTitle": [{"@type": "Title", "mainTitle": "OoB"}],
                        }
                    ],
                    "part": ["1927, s. 1-10"],
                    "hasNote": [
                        {
                            "@type": "Note",
                            "label": [
                                "Fullständig beskrivning (OCR) ur SHBD: Bengtsson, Frans G., Karl XII. (OoB 1927, s. 1-10.)"
                            ],
                        }
                    ],
                },
            ]
        },
        "libris_rec": {
            "@graph": [
                {
                    "@id": "https://libris.kb.se/dwpd26rq5r949g2",
                    "@type": "Record",
                    "sameAs": [{"@id": "http://libris.kb.se/bib/2982652"}],
                    "created": "2001-12-11T00:00:00+01:00",
                    "modified": "2003-02-17T11:39:51+01:00",
                    "mainEntity": {"@id": "https://libris.kb.se/dwpd26rq5r949g2#it"},
                    "identifiedBy": [
                        {"@type": "LibrisIIINumber", "value": "9931291427"}
                    ],
                    "recordStatus": "marc:CorrectedOrRevised",
                    "controlNumber": "2982652",
                    "encodingLevel": "marc:AbbreviatedLevel",
                    "generationDate": "2026-03-05T15:16:35.567+01:00",
                    "generationProcess": {
                        "@id": "https://libris.kb.se/sys/globalchanges/typenormalization/main-skip-modified.groovy"
                    },
                    "descriptionCreator": {"@id": "https://libris.kb.se/library/ARJ"},
                    "marc:catalogingSource": {
                        "@id": "https://id.kb.se/marc/CooperativeCatalogingProgram"
                    },
                },
                {
                    "@id": "https://libris.kb.se/dwpd26rq5r949g2#it",
                    "@type": "PhysicalResource",
                    "sameAs": [{"@id": "http://libris.kb.se/resource/bib/2982652"}],
                    "category": [{"@id": "https://id.kb.se/term/saobf/Print"}],
                    "hasTitle": [
                        {
                            "@type": "Title",
                            "subtitle": "Overs. av Trygve Width.",
                            "mainTitle": "Karl XII.",
                        }
                    ],
                    "instanceOf": {
                        "@type": "Monograph",
                        "category": [
                            {"@id": "https://id.kb.se/term/ktg/NonFictionLiterature"},
                            {"@id": "https://id.kb.se/term/rda/Text"},
                        ],
                        "language": [{"@id": "https://id.kb.se/language/nor"}],
                        "contribution": [
                            {
                                "role": {"@id": "https://id.kb.se/relator/author"},
                                "@type": "PrimaryContribution",
                                "agent": {
                                    "@id": "https://libris.kb.se/b8nqqfvv43djcm1#it"
                                },
                            }
                        ],
                    },
                    "publication": [
                        {
                            "year": "1949",
                            "@type": "PrimaryPublication",
                            "place": [{"@type": "Place", "label": ["Oslo"]}],
                            "country": {"@id": "https://id.kb.se/country/no"},
                        }
                    ],
                },
            ],
            "@context": "/context.jsonld",
        },
        "expected_to_match": False,
        "description": "6 träffar från API. Denna verkar vara en senare översättning.",
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
