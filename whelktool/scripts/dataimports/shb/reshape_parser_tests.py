import pytest

from reshape_shb import (
    parse_note,
    extract_structured_values,
    extract_partof_from_parenthesis,
    extract_extent,
)

### End-to-end parsing ###


def test_parse_note_1771_1874_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [
            {
                "pattern": "Efternamn, Förnamn [initial]., Titel. Publ-titel YYYY, nr (DD/MM).",
                "label": "Holm, C. J., Också några ord om slaget vid Porosalmiden 12 juni 1789. Svenska Tidningen 1853, N:o 72 (81/8),"
            }
        ]
    }

    result = parse_note(instance, "early")

    assert (
        result["hasTitle"]["mainTitle"]
        == "Också några ord om slaget vid Porosalmiden 12 juni 1789"
    )
    assert result["responsibilityStatement"] == "Holm, C. J."
    assert result["hasNote"][0]["label"] == "Svenska Tidningen 1853, N:o 72 (81/8),"
    # TODO? assert result["partOf"][0]["label"] == ...



def test_parse_note_1875_1900_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [
            {"pattern": "Efternamn, Förnamn [initial]., Titel. Publ-titel YYYY, nr.",
             "label": "Hagemann, A., Et historisk Minde i Höifjeldet. [1657.] Aftenposten (Kristiania) 1897, N:r 186."}
        ]
    }

    result = parse_note(instance, "early")

    assert result["hasTitle"]["mainTitle"] == "Et historisk Minde i Höifjeldet"
    assert result["responsibilityStatement"] == "Hagemann, A."
    assert result["hasNote"][0]["label"] == "[1657.] Aftenposten (Kristiania) 1897, N:r 186."
    # TODO? assert result["partOf"][0]["label"] == ...


def test_parse_note_1936_1950_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [
            {
                "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. (Publ-titel DD/MM YYYY.)",
                "label": "Ahnlund, N., Rikskanslerns titlar. [Axel Oxenstierna.] (SvD 14/7 1939.)"
            }
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["hasTitle"]["mainTitle"] == "Rikskanslerns titlar"
    assert result["responsibilityStatement"] == "Ahnlund, N."
    assert result["hasNote"][0]["label"] == "[Axel Oxenstierna.] (SvD 14/7 1939.)"


def test_parse_note_1936_1950_Bidrag():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn [initial]., Titel. undertitel. (Publ-titel nr (årtal), sid.)"
            }
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1936_1950_Monografi():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn [initial]., Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)"
            }
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1901_1920_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn [initial]., Titel. undertitel. Publ-titel YYYY, numrering (DD/MM)."
            }
        ]
    }

    result = parse_note(instance, "early")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1771_1874_1875_1900_1901_1920_Bidrag():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn [initial]., Titel. undertitel. Publ-titel. nr, sid."
            }
        ]
    }

    result = parse_note(instance, "early")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1771_1874_1875_1900_1901_1920_Monografi():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn [initial]., Titel. undertitel. sid. Ort år. Serietillhörighet. numrering."
            }
        ]
    }

    result = parse_note(instance, "early")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1971_1975_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel DD.MM YYYY"
            }
        ]
    }

    result = parse_note(instance, "isbd_transition")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1976_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel DD.MM.YYYY"
            }
        ]
    }

    result = parse_note(instance, "isbd")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1971_1975_Bidrag():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel årg(årtal):numrering, sid."
            }
        ]
    }

    result = parse_note(instance, "isbd_transition")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1976_Bidrag():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel, ISSN, numrering, årg, sid."
            }
        ]
    }

    result = parse_note(instance, "isbd")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1976_Monografi():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel : undertitel. - Ort, år. - sid. - (Serietillhörighet, ISSN)"
            }
        ]
    }

    result = parse_note(instance, "isbd")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1971_1975_Monografi():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel : undertitel. Ort år. sid. - (Serietillhörighet ; numrering)"
            }
        ]
    }

    result = parse_note(instance, "isbd_transition")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1961_1970_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [{"label": "Efternamn, Förnamn, Titel. - Publ-titel DD.MM YYYY."}]
    }

    result = parse_note(instance, "dash_style")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1961_1970_Bidrag():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel. - Publ-titel årg (årtal):numrering, sid."
            }
        ]
    }

    result = parse_note(instance, "dash_style")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1951_1960_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [
            {"label": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel DD/MM YYYY.)"}
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1921_1935_Bidrag_tidningsartikel():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel YYYY: DD/MM.)"
            }
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1921_1935_1951_1960_Bidrag():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel nr (årtal), sid.)"
            }
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1921_1935_Monografi():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)"
            }
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1961_1970_Monografi():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel. undertitel. Ort år. sid. - Serietillhörighet. numrering."
            }
        ]
    }

    result = parse_note(instance, "dash_style")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1951_1960_Monografi():
    instance = {
        "hasNote": [
            {
                "label": "Efternamn, Förnamn, Titel. undertitel. sid. Ort år. (Serietillhörighet. numrering.)"
            }
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1971_1975_Bidrag_utan_författare():
    instance = {
        "hasNote": [
            {
                "label": "Titel : undertitel / Upphov. - I: Publ-titel årg(årtal):numrering, sid."
            }
        ]
    }

    result = parse_note(instance, "isbd_transition")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1976_Bidrag_utan_författare():
    instance = {
        "hasNote": [
            {
                "label": "Titel : undertitel / Upphov. - I: Publ-titel, ISSN, numrering, årg, sid."
            }
        ]
    }

    result = parse_note(instance, "isbd")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1976_Monografi_utan_författare():
    instance = {
        "hasNote": [{"label": "Titel : undertitel / Upphov. - Ort, år. - sid."}]
    }

    result = parse_note(instance, "isbd")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1971_1975_Monografi_utan_författare():
    instance = {
        "hasNote": [
            {
                "label": "Titel : undertitel / Upphov. Ort år. sid. - (Serietillhörighet ; numrering)"
            }
        ]
    }

    result = parse_note(instance, "isbd_transition")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1961_1970_Bidrag_utan_författare():
    instance = {
        "hasNote": [{"label": "Titel. - Publ-titel årg (årtal):numrering, sid."}]
    }

    result = parse_note(instance, "dash_style")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1921_1935_1936_1950_Bidrag_utan_författare():
    instance = {
        "hasNote": [{"label": "Titel. undertitel. (Publ-titel. nr. årg, sid.)"}]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1921_1935_1936_1950_Monografi_utan_författare():
    instance = {
        "hasNote": [
            {"label": "Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)"}
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1961_1970_Monografi_utan_författare():
    instance = {
        "hasNote": [
            {"label": "Titel. undertitel. Ort år. sid. - Serietillhörighet. numrering."}
        ]
    }

    result = parse_note(instance, "dash_style")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1771_1874_1875_1900_1901_1920_Bidrag_utan_författare():
    instance = {"hasNote": [{"label": "Titel. undertitel. Publ-titel. nr, sid."}]}

    result = parse_note(instance, "early")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1951_1960_Monografi_utan_författare():
    instance = {
        "hasNote": [
            {"label": "Titel. undertitel. sid. Ort år. (Serietillhörighet. numrering.)"}
        ]
    }

    result = parse_note(instance, "parenthesized")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


def test_parse_note_1771_1874_1875_1900_1901_1920_Monografi_utan_författare():
    instance = {
        "hasNote": [
            {"label": "Titel. undertitel. sid. Ort år. Serietillhörighet. numrering."}
        ]
    }

    result = parse_note(instance, "early")

    assert result["category"] == None
    assert result["hasTitle"]["mainTitle"] == None
    assert result["hasTitle"]["subtitle"] == None
    assert result["responsibilityStatement"] == None
    assert result["extent"] == None
    assert result["partOf"] == None
    assert result["seriesMembership"] == None
    assert result["hasNote"] == None


### Parsing for specific values ###
# Main entity values
EXTRACT_STRUCTURED_TEST_CASES = {
    "simple-author-with-initials": (
        "Surname, G.-N., Anything",
        (
            "Surname, G.-N.",
            "Anything",
            None,
            None,
            None,
            None,
            False,
        ),
    ),
    "simple-title-before-author": (
        "Anything. Surname, G.-N., Stuff.",
        (
            None,
            "Anything",
            None,
            None,
            None,
            "Surname, G.-N., Stuff.",
            False,
        ),
    ),
    "publication-year-before-monograph-extent": (
        "Schuck, A., H. Schücks enka & Co. AB 150 år. [Stockholm.] Sthlm 1947, 28 s.",
        (
            "Schuck, A.",
            "H. Schücks enka & Co. AB 150 år",
            None,
            "28 s.",
            None,
            "[Stockholm.] Sthlm 1947",
            False,
        ),
    ),
    "component-part-extent-in-parentheses": (
        "Meyerson, Å., Ett besök vid Stora Kopparberget och Sala gruva år 1662. (BBV 23 (1938), s. 325-343.)",
        (
            "Meyerson, Å.",
            "Ett besök vid Stora Kopparberget och Sala gruva år 1662",
            None,
            "s. 325-343",
            None,
            "(BBV 23 (1938).)",
            True,
        ),
    ),
    "component-part-with-subtitle-and-series": (
        'Davidsson, Åke, "En hoop Discantzböcker i godt förhwar..." : någotom Strängnäsgymnasiets musiksamling under 1600-talet. - I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976, s. 48-62',
        (
            "Davidsson, Åke",
            '"En hoop Discantzböcker i godt förhwar..."',
            "någotom Strängnäsgymnasiets musiksamling under 1600-talet",
            "s. 48-62",
            None,
            "I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976",
            True,
        ),
    ),
    "component-part-without-author-with-issn": (
        "Barton, H. Arnold, A bibliography of writings in English by or onrecent Swedish emigration historians. - I: The Swedish pioneer, ISSN0039-7326, 27, 1976:3, s. 215-221",
        (
            "Barton, H. Arnold",
            "A bibliography of writings in English by or onrecent Swedish emigration historians",
            None,
            "s. 215-221",
            "0039-7326",
            "I: The Swedish pioneer, ISSN0039-7326, 27, 1976:3",
            True,
        ),
    ),
    "monograph-with-contributors-written-two-ways": (
        "Jonsson, Inge, Swedenborg : sökaren i naturens och andens värld :hans verk och efterföljd / Inge Jonsson, Olle Hjern. -Stockholm, 1976. - 187 s.Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin",
        (
            "Jonsson, Inge",
            "Swedenborg",
            "sökaren i naturens och andens värld :hans verk och efterföljd",
            "187 s.",
            None,
            "Inge Jonsson, Olle Hjern. -Stockholm, 1976. -Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin",
            False,
        ),
    ),
    "monograph-with-series-statement-with-issn": (
        "Fries, Elias, Hembygdsperiodika : förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl. - Borås, 1976. - 40 bl. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)",
        (
            "Fries, Elias",
            "Hembygdsperiodika",
            "förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl",
            "40 bl.",
            "0347-1128",
            "Borås, 1976. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)",
            False,
        ),
    ),
    "monograph-with-series-and-dissertation-note-with-issn": (
        "Edvardsson, Lars, Kyrka och judendom : svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975. -Lund, 1976. - 194 s. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed",
        (
            "Edvardsson, Lars",
            "Kyrka och judendom",
            "svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975",
            "194 s.",
            "0346-5438",
            "Lund, 1976. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed",
            False,
        ),
    ),
    "monograph-without-secondary-title-with-issn": (
        "Frithz, Carl-Gösta, Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning. - Lund, 1976. - 428 s. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec",
        (
            "Frithz, Carl-Gösta",
            "Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning",
            None,
            "428 s.",
            "0519-9859",
            "Lund, 1976. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec",
            False,
        ),
    ),
    "multiple-authors-and-complex-multi-part-extent": (
        "Erichsen, B., & Krarup, A., Dansk historisk Bibliografi. Bd 1-3. Khvn1918-21, 1925-27, 1917. xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s.",
        (
            "Erichsen, B., & Krarup, A.",
            "Dansk historisk Bibliografi. Bd 1-3",
            None,
            "xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s.",
            None,
            "Khvn1918-21, 1925-27, 1917.",
            False,
        ),
    ),
}


@pytest.mark.parametrize("case_name", EXTRACT_STRUCTURED_TEST_CASES)
def test_extract_structured_values(case_name):
    record, expected = EXTRACT_STRUCTURED_TEST_CASES[case_name]

    actual = extract_structured_values(record, False)

    assert actual == expected, f"\nInput record:\n{record}"


# Part-of from parenthesis
@pytest.mark.parametrize(
    "record, era, expected",
    [
        pytest.param(
            "isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287.) Stockholm",
            "era_2",
            (
                "isborgs slott.  Stockholm",
                "Antikvariska studier. 4. Sthlm 1950, s. 221-287.",
            ),
            id="simple-parenthesized-partof",
        ),
        pytest.param(
            "isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. (VHAAH 71.))",
            "era_2",
            (
                "isborgs slott.",
                "Antikvariska studier. 4. Sthlm 1950, s. 221-287. (VHAAH 71.)",
            ),
            id="nested-parentheses-inside-partof",
        ),
        pytest.param(
            "isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. VHAAH 71.))",
            "era_2",
            (
                "isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. VHAAH 71.))",
                None,
            ),
            id="unbalanced-parentheses-not-extracted",
        ),
        pytest.param(
            "isborgs slott.",
            "era_2",
            (
                "isborgs slott.",
                None,
            ),
            id="no-parentheses",
        ),
    ],
)
def test_extract_partof_from_parenthesis(record, era, expected):
    actual = extract_partof_from_parenthesis(record, era)

    assert actual == expected, f"\nInput record:\n{record}"


# Extent
@pytest.mark.parametrize(
    "note, expected",
    [
        pytest.param(
            "Antikvariska studier. 4. Sthlm 1950, s. 221-287.",
            ("s. 221-287", "Antikvariska studier. 4. Sthlm 1950.", True),
            id="simple-component-extent",
        )
    ],
)
def test_extract_extent(note, expected):
    actual = extract_extent(note, False)

    assert actual == expected, f"\nInput record:\n{note}"


### Known failing tests ###
@pytest.mark.xfail(
    reason="Surname-like title beginning is currently misidentified as author"
)
def test_slott_svenska_title_start():
    record = "Slott, Svenska, och herresäten vid 1900-talets början."

    result = extract_structured_values(record, False)

    assert result[0] is None
    assert result[1] == "Slott, Svenska, och herresäten vid 1900-talets början."


@pytest.mark.xfail(
    reason="If removing a year that has been confused with extent leads to the extent just being 's', give up on extrating the extent."
)
def test_extent_dot_year(note, expected):
    note = "524, (2) s. 1951."
    result = extract_extent(note, False)
    assert result == ""
