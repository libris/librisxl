import pytest
from reshape_shb import (
    extract_extent,
    extract_partof_from_parenthesis,
    extract_structured_values,
    parse_note,
)

### End-to-end parsing ###

# TODO or not todo?
# Parse out series/partOf info for older descriptions (no parenthesis or "I:" to go by)
# Identify contributions (bidrag) when there is no extent to go by


def test_extract_structured_values_1771_1874_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. Publ-titel YYYY, nr (DD/MM).",
        "sample": "Holm, C. J., Också några ord om slaget vid Porosalmiden 12 juni 1789. Svenska Tidningen 1853, N:o 72 (81/8),",
    }

    result = extract_structured_values(record["sample"], "early")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Holm, C. J."
    assert result["title"] == "Också några ord om slaget vid Porosalmiden 12 juni 1789"
    assert result["remaining_note"] == "Svenska Tidningen 1853, N:o 72 (81/8)"


def test_extract_structured_values_1875_1900_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. Publ-titel YYYY, nr.",
        "sample": "Hagemann, A., Et historisk Minde i Höifjeldet. [1657.] Aftenposten (Kristiania) 1897, N:r 186.",
    }

    result = extract_structured_values(record["sample"], "early")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Hagemann, A."
    assert result["title"] == "Et historisk Minde i Höifjeldet"
    assert result["remaining_note"] == "[1657.] Aftenposten (Kristiania) 1897, N:r 186"


def test_extract_structured_values_1936_1950_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. (Publ-titel DD/MM YYYY.)",
        "sample": "Ahnlund, N., Rikskanslerns titlar. [Axel Oxenstierna.] (SvD 14/7 1939.)",
    }

    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1936_1950_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. (Publ-titel nr (årtal), sid.)",
        "sample": "Walde, O., Bielkeättens insatser i svensk bibliofili. Med särskild hänsyn till Bielkebiblioteket på Skokloster. (NTBB 27 (1940), s. 1-45.)",
    }

    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Walde, O."
    assert result["title"] == "Bielkeättens insatser i svensk bibliofili"
    assert (
        result["remaining_note"]
        == "Med särskild hänsyn till Bielkebiblioteket på Skokloster"
    )
    assert result["host"] == {
        "title": "NTBB",
        "part_number": "27 (1940)",
        "extent": "s. 1-45",
    }


def test_extract_structured_values_1936_1950_Monografi():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)",
        "sample": "Weibull, C, Händelser och utvecklingslinjer. Historiska studier. Lund 1949,  254 s. (Göteborgs högskola. Forskningar och föreläsningar.) Rec. i HT 70 (1950), s. 69-70 av T. [T:son] H[öjer]; i SvD 21/11 1949 av  dens.; i SDS 25/11 1949 av K.-E. L[öfqvist]; i FT 148 (1950), s. 59-61 av 0.  M[usteli]n; i StT 1/12 1949 av S. U. Palme; i GHT 7/12 1949 av K[nut] P[etersson]. ",
    }

    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Weibull, C"
    assert result["title"] == "Händelser och utvecklingslinjer. Historiska studier"
    assert result["place"] == "Lund"
    assert result["year"] == "1949"
    assert result["extent"] == "254 s."
    assert (
        result["remaining_note"]
        == "Rec. i HT 70 (1950), s. 69-70 av T. [T:son] H[öjer]; i SvD 21/11 1949 av dens.; i SDS 25/11 1949 av K.-E. L[öfqvist]; i FT 148 (1950), s. 59-61 av 0. M[usteli]n; i StT 1/12 1949 av S. U. Palme; i GHT 7/12 1949 av K[nut] P[etersson]"
    )
    assert result["host"] == {
        "title": "Göteborgs högskola. Forskningar och föreläsningar"
    }


def test_extract_structured_values_1901_1920_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. Publ-titel YYYY, numrering (DD/MM).",
        "sample": "Stridsberg, G., En svensk kulturbild. Sveriges historia och kammararkivets traditioner. Svenska Dagbladet 1908, N:o 127 (n/6).",
    }
    result = extract_structured_values(record["sample"], "early")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Stridsberg, G."
    assert (
        result["title"]
        == "En svensk kulturbild. Sveriges historia och kammararkivets traditioner"
    )
    assert result["remaining_note"] == "Svenska Dagbladet 1908, N:o 127 (n/6)"


def test_extract_structured_values_1771_1874_1875_1900_1901_1920_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. Publ-titel. nr, sid.",
        "sample": "Staven OW, L., Om förhållandet mellan politisk historia och kultur- historia. Hist, Tidskr. 1895, s. 415-430.",
    }

    result = extract_structured_values(record["sample"], "early")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Staven OW, L."
    assert (
        result["title"]
        == "Om förhållandet mellan politisk historia och kultur- historia. Hist, Tidskr"
    )
    assert result["remaining_note"] == "1895"
    assert result["extent"] == "s. 415-430"


def test_extract_structured_values_1771_1874_1875_1900_1901_1920_Monografi():
    record = {
        "key": "Efternamn, Förnamn [initial]., Titel. undertitel. sid. Ort år. Serietillhörighet. numrering.",
        "sample": "Silfverstolpe, C, Klosterfolket i Vadstena. Personhistoriska anteck- ningar. H. 1-2. 167+ (1) s. +1 pl. Sthlm 1898, 99. Skrifter och handlingar utgifna genom Svenska Autografsällskapet. 4.",
    }
    result = extract_structured_values(record["sample"], "early")

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Silfverstolpe, C"
    assert result["extent"] == "167+ (1) s. +1 pl."
    assert (
        result["title"]
        == "Klosterfolket i Vadstena. Personhistoriska anteck- ningar. H. 1-2. Sthlm 1898, 99. Skrifter och handlingar utgifna genom Svenska Autografsällskapet"
    )


def test_extract_structured_values_1971_1975_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel DD.MM YYYY",
        "sample": "Bramstång, Mats, Glimtar av ett helgon [Birgitta]. - I: HbgD 6.10 1073",
    }
    result = extract_structured_values(record["sample"], "isbd_transition")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Bramstång, Mats"
    assert result["title"] == "Glimtar av ett helgon [Birgitta]"
    assert result["host"] == {"title": "HbgD", "part_number": "6.10 1073"}


def test_extract_structured_values_1976_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel DD.MM.YYYY",
        "sample": "Palm, Sixten, Apropå slaget vid Lund. - I: SDS 14.3.1976",
    }
    result = extract_structured_values(record["sample"], "isbd")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Palm, Sixten"
    assert result["title"] == "Apropå slaget vid Lund"
    assert result["host"] == {"title": "SDS", "part_number": "14.3.1976"}


def test_extract_structured_values_1971_1975_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel årg(årtal):numrering, sid.",
        "sample": "Jansson, Bror, Helsjön — en berömd kuranstalt vid Västgötagränsen. — I: Vår bygd 56(1973), s. 49-59 : ill.",
    }
    result = extract_structured_values(record["sample"], "isbd_transition")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Jansson, Bror"
    assert result["title"] == "Helsjön - en berömd kuranstalt vid Västgötagränsen"
    assert result["host"] == {
        "title": "Vår bygd",
        "part_number": "56(1973)",
        "extent": "s. 49-59 : ill.",
    }


def test_extract_structured_values_1976_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel, ISSN, numrering, årg, sid.",
        "sample": "Seitz, Heribert, Ett hundraårigt beslut : hur Armémuseumgrundades. - I: Meddelande / Armémuseum, ISSN 0349-1048, 37,1976/1977, s. 117-123",
    }
    result = extract_structured_values(record["sample"], "isbd")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Seitz, Heribert"
    assert result["title"] == "Ett hundraårigt beslut"
    assert result["subtitle"] == "hur Armémuseumgrundades"
    assert result["host"] == {
        "title": "Meddelande",
        "publisher": "Armémuseum",
        "issn": "0349-1048",
        "part_number": "37,1976/1977",
        "extent": "s. 117-123",
    }


def test_extract_structured_values_1976_Monografi():
    record = {
        "sample": "Larsson, Gunilla, Curt Weibull : en bibliografi 19 augusti 1976. -Stockholm, 1976. - 27 s. - (Acta Bibliothecae regiae Stockholmiensis,ISSN 0065-1060 ; 28)"
    }
    result = extract_structured_values(record["sample"], "isbd")

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Larsson, Gunilla"
    assert result["title"] == "Curt Weibull"
    assert result["subtitle"] == "en bibliografi 19 augusti 1976"
    assert result["place"] == "Stockholm"
    assert result["year"] == "1976"
    assert result["extent"] == "27 s."
    assert result["host"] == {
        "title": "Acta Bibliothecae regiae Stockholmiensis",
        "issn": "0065-1060",
        "part_number": "28",
    }


def test_extract_structured_values_1971_1975_Monografi():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. Ort år. sid. - (Serietillhörighet ; numrering)",
        "sample": "WeibuII, Curt, Die Geaten des Beowulfepos und Die dänischen Trelle-burgen : zwei Diskussionsbeiträge. Göteborg 1974. 43 s. - (Acta RegiaeSocietatis scientiarum et litterarum Gothoburgensis. Humaniora ; 10)",
    }
    result = extract_structured_values(record["sample"], "isbd_transition")

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "WeibuII, Curt"
    assert (
        result["title"] == "Die Geaten des Beowulfepos und Die dänischen Trelle-burgen"
    )
    assert result["subtitle"] == "zwei Diskussionsbeiträge"
    assert result["place"] == "Göteborg"
    assert result["year"] == "1974"
    assert result["extent"] == "43 s."
    assert result["host"] == {
        "title": "Acta RegiaeSocietatis scientiarum et litterarum Gothoburgensis. Humaniora",
        "part_number": "10",
    }


def test_extract_structured_values_1961_1970_Bidrag_tidningsartikel():
    record = {"pattern": "Efternamn, Förnamn, Titel. - Publ-titel DD.MM YYYY.",
              "sample": "Jansson, Leonard, Ivar Axelsson Tott. - ÖresP 2.6 1962."}

    result = extract_structured_values(record["sample"], "dash_style")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Jansson, Leonard"
    assert result["title"] == "Ivar Axelsson Tott"
    assert result["remaining_note"] == "ÖresP 2.6 1962"

def test_extract_structured_values_1961_1970_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. - Publ-titel årg (årtal):numrering, sid.",
        "sample": "Lagerroth, Fredrik, Positiv rätt eller naturrätt? Ett statsrattsligt dilemma från svenskt 1700-tal. [Zusammenfassung.] - Scandia 33 (1967), s. 270-312."
    }
    result = extract_structured_values(record["sample"], "dash_style")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Lagerroth, Fredrik"
    assert result["title"] == "Positiv rätt eller naturrätt? Ett statsrattsligt dilemma från svenskt 1700-tal"
    assert result["extent"] == "s. 270-312"
    assert result["remaining_note"] == "[Zusammenfassung.] - Scandia 33 (1967)"

def test_extract_structured_values_1951_1960_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel DD/MM YYYY.)",
        "sample": "Ennermark, S., Brasks boktryckare en pionjär i Malmö. (SDS 31/8 1952.)"
    }
    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ennermark, S."
    assert result["title"] == "Brasks boktryckare en pionjär i Malmö"
    assert result["host"] == {"title": "SDS", "part_number": "31/8 1952"}


def test_extract_structured_values_1921_1935_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel YYYY: DD/MM.)",
        "sample": "Steckzén, B., Med svärdet och plogen. (SvD 1935: 2/9.)"
    }
    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Steckzén, B."
    assert result["title"] == "Med svärdet och plogen"
    assert result["host"] == {"title": "SvD", "part_number": "1935: 2/9"}


def test_extract_structured_values_1921_1935_1951_1960_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel nr (årtal), sid.)",
        "sample": "Berg, Gösta, Svensk folklivskännedom. En översikt av de senare årenslitteratur. (Hävd och hembygd. 2 (1927), s. 76-95.)"
    }
    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Berg, Gösta"
    assert result["title"] == "Svensk folklivskännedom"
    assert result["remaining_note"] == "En översikt av de senare årenslitteratur"
    assert result["host"] == {"title": "Hävd och hembygd", "part_number": "2 (1927)", "extent": "s. 76-95"}


def test_extract_structured_values_1921_1935_Monografi():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)",
        "sample": "Nohrstrom, Holger, Borgå gymnasiebibliotek och dess föregångare blandFinlands läroverksbibliotek. Ett bidrag till Finlands biblioteks- och kulturhistoria.Hfors 1927. (2), 291 s. (Helsingfors universitetsbiblioteks skrifter.10.)"
    }
    result = extract_structured_values(record["sample"], "parenthesized")

    print(result)

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Nohrstrom, Holger"
    assert result["title"] == "Borgå gymnasiebibliotek och dess föregångare blandFinlands läroverksbibliotek. Ett bidrag till Finlands biblioteks- och kulturhistoria"
    assert result["place"] == "Hfors"
    assert result["year"] == "1927"
    assert result["extent"] == "(2), 291 s."
    assert result["host"] == {"title": "Helsingfors universitetsbiblioteks skrifter", "part_number": "10"}


def test_extract_structured_values_1961_1970_Monografi():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. Ort år. sid. - Serietillhörighet. numrering.",
        "sample": "Clemedson, Carl-Johan, Taxinge socken. Kultur, vegetation, flora. Nyköping 1970. 93 s. Ill. - Sörmländska handlingar. 27."
    }
    result = extract_structured_values(record["sample"], "dash_style")

    print(result)

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Clemedson, Carl-Johan"
    assert result["title"] == "Taxinge socken. Kultur, vegetation, flora."
    assert result["place"] == "Nyköping"
    assert result["year"] == "1970"
    assert result["extent"] == "93 s. Ill."
    assert result["remaining_note"] == "Sörmländska handlingar. 27"

def test_extract_structured_values_1951_1960_Monografi():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. sid. Ort år. (Serietillhörighet. numrering.)",
        "sample": "Davidsson, Åke, Handritade kartor över Finland i Uppsala universitetsbibliotek.  60 s., 3 pl.-bl. Uppsala 1957. (Acta Bibliothecae R. Universitatis Upsaliensis. 11.)"
    }
    result = extract_structured_values(record["sample"], "parenthesized")

    print(result)

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Davidsson, Åke"
    assert result["title"] == "Handritade kartor över Finland i Uppsala universitetsbibliotek"
    assert result["extent"] == "60 s., 3 pl.-bl."
    assert result["palce"] == "Uppsala"
    assert result["year"] == "1957"
    assert result["host"] == {"title": "Acta Bibliothecae R. Universitatis Upsaliensis", "part_number": "11"}


def test_extract_structured_values_1971_1975_Bidrag_utan_författare():
    record = {
        "sample": "Titel : undertitel / Upphov. - I: Publ-titel årg(årtal):numrering, sid."
    }
    result = extract_structured_values(record["sample"], "isbd_transition")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1976_Bidrag_utan_författare():
    record = {
        "sample": "Titel : undertitel / Upphov. - I: Publ-titel, ISSN, numrering, årg, sid."
    }
    result = extract_structured_values(record["sample"], "isbd")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1976_Monografi_utan_författare():
    record = {"sample": "Titel : undertitel / Upphov. - Ort, år. - sid."}

    result = extract_structured_values(record["sample"], "isbd")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1971_1975_Monografi_utan_författare():
    record = {
        "sample": "Titel : undertitel / Upphov. Ort år. sid. - (Serietillhörighet ; numrering)"
    }
    result = extract_structured_values(record["sample"], "isbd_transition")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1961_1970_Bidrag_utan_författare():
    record = {
        "hasNote": [{"sample": "Titel. - Publ-titel årg (årtal):numrering, sid."}]
    }

    result = extract_structured_values(record["sample"], "dash_style")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1921_1935_1936_1950_Bidrag_utan_författare():
    record = {"sample": "Titel. undertitel. (Publ-titel. nr. årg, sid.)"}

    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1921_1935_1936_1950_Monografi_utan_författare():
    record = {
        "sample": "Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)"
    }
    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1961_1970_Monografi_utan_författare():
    record = {
        "sample": "Titel. undertitel. Ort år. sid. - Serietillhörighet. numrering."
    }
    result = extract_structured_values(record["sample"], "dash_style")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1771_1874_1875_1900_1901_1920_Bidrag_utan_författare():
    record = {"sample": "Titel. undertitel. Publ-titel. nr, sid."}

    result = extract_structured_values(record["sample"], "early")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1951_1960_Monografi_utan_författare():
    record = {
        "sample": "Titel. undertitel. sid. Ort år. (Serietillhörighet. numrering.)"
    }
    result = extract_structured_values(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_extract_structured_values_1771_1874_1875_1900_1901_1920_Monografi_utan_författare():
    record = {"sample": "Titel. undertitel. sid. Ort år. Serietillhörighet. numrering."}

    result = extract_structured_values(record["sample"], "early")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["remaining_note"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


### Parsing for specific values ###
# Main entity values
EXTRACT_STRUCTURED_VALUES_TEST_CASES = {
    "simple-author-with-initials": (
        "Surname, G.-N., Anything",
        "early",
        {
            "primary_contributors": "Surname, G.-N.",
            "title": "Anything",
            "is_component_part": True,
        },
    ),
    "publication-year-before-monograph-extent": (
        "Schuck, A., H. Schücks enka & Co. AB 150 år. [Stockholm.] Sthlm 1947, 28 s.",
        "parenthesized",
        {
            "primary_contributors": "Schuck, A.",
            "title": "H. Schücks enka & Co. AB 150 år",
            "extent": "28 s.",
            "place": "[Stockholm.] Sthlm",
            "year": "1947",
            "is_component_part": False,
        },
    ),
    "component-part-extent-in-parentheses": (
        "Meyerson, Å., Ett besök vid Stora Kopparberget och Sala gruva år 1662. (BBV 23 (1938), s. 325-343.)",
        "parenthesized",
        {
            "primary_contributors": "Meyerson, Å.",
            "title": "Ett besök vid Stora Kopparberget och Sala gruva år 1662",
            "host": {
                "title": "BBV",
                "part_number": "23 (1938)",
                "extent": "s. 325-343",
            },
            "is_component_part": True,
        },
    ),
    "component-part-with-subtitle-and-series": (
        'Davidsson, Åke, "En hoop Discantzböcker i godt förhwar..." : någotom Strängnäsgymnasiets musiksamling under 1600-talet. - I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976, s. 48-62',
        "isbd",
        {
            "primary_contributors": "Davidsson, Åke",
            "title": '"En hoop Discantzböcker i godt förhwar..."',
            "subtitle": "någotom Strängnäsgymnasiets musiksamling under 1600-talet",
            "host": {
                "title": "Frånbiskop Rogge till Roggebiblioteket. Nyköping",
                "part_number": "1976",
                "extent": "s. 48-62",
            },
            "is_component_part": True,
        },
    ),
    "component-part-without-author-with-issn": (
        "Barton, H. Arnold, A bibliography of writings in English by or onrecent Swedish emigration historians. - I: The Swedish pioneer, ISSN0039-7326, 27, 1976:3, s. 215-221",
        "isbd",
        {
            "primary_contributors": "Barton, H. Arnold",
            "title": "A bibliography of writings in English by or onrecent Swedish emigration historians",
            "host": {
                "title": "The Swedish pioneer",
                "part_number": "27, 1976:3",
                "issn": "0039-7326",
                "extent": "s. 215-221",
            },
            "is_component_part": True,
        },
    ),
    "monograph-with-primary-and-other-contributors": (
        "Jonsson, Inge, Swedenborg : sökaren i naturens och andens värld :hans verk och efterföljd / Inge Jonsson, Olle Hjern. -Stockholm, 1976. - 187 s.Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin",
        "isbd",
        {
            "primary_contributors": "Jonsson, Inge",
            "other_contributors": "Inge Jonsson, Olle Hjern",
            "title": "Swedenborg",
            "subtitle": "sökaren i naturens och andens värld :hans verk och efterföljd",
            "extent": "187 s.",
            "place": "Stockholm",
            "year": "1976",
            "remaining_note": "Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S.Stolpe; i DN 11.11.1977 av I. Algulin",
            "is_component_part": False,
        },
    ),
    "monograph-with-series-statement-with-issn": (
        "Fries, Elias, Hembygdsperiodika : förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl. - Borås, 1976. - 40 bl. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)",
        "isbd",
        {
            "primary_contributors": "Fries, Elias",
            "title": "Hembygdsperiodika",
            "subtitle": "förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl",
            "extent": "40 bl.",
            "place": "Borås",
            "year": "1976",
            "host": {
                "title": "Specialarbete",
                "publisher": "Bibliotekshögskolan",
                "part_number": "1976:158",
                "issn": "0347-1128",
            },
            "is_component_part": False,
        },
    ),
    "monograph-with-series-and-dissertation-note-with-issn": (
        "Edvardsson, Lars, Kyrka och judendom : svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975. -Lund, 1976. - 194 s. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed",
        "isbd",
        {
            "primary_contributors": "Edvardsson, Lars",
            "title": "Kyrka och judendom",
            "subtitle": "svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975",
            "extent": "194 s.",
            "place": "Lund",
            "year": "1976",
            "host": {
                "title": "Bibliotheca historico-ecclesiasticaLundensis",
                "part_number": "6",
                "issn": "0346-5438",
            },
            "remaining_note": "Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed",
            "is_component_part": False,
        },
    ),
    "monograph-without-secondary-title-with-issn": (
        "Frithz, Carl-Gösta, Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning. - Lund, 1976. - 428 s. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av S. Helander",
        "isbd",
        {
            "primary_contributors": "Frithz, Carl-Gösta",
            "title": "Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning",
            "extent": "428 s.",
            "place": "Lund",
            "year": "1976",
            "host": {
                "title": "Bibliothecatheologiae practicae",
                "part_number": "34",
                "issn": "0519-9859",
            },
            "remaining_note": "Diss. Mit deutscherZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av S. Helander",
            "is_component_part": False,
        },
    ),
    "multiple-authors-and-complex-multi-part-extent": (
        "Erichsen, B., & Krarup, A., Dansk historisk Bibliografi. Bd 1-3. Khvn1918-21, 1925-27, 1917. xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s.",
        "early",
        {
            "primary_contributors": "Erichsen, B., & Krarup, A.",
            "title": "Dansk historisk Bibliografi. Bd 1-3",
            "extent": "xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s.",
            "remaining_note": "Khvn1918-21, 1925-27, 1917",
            "is_component_part": False,
        },
    ),
}


@pytest.mark.parametrize("case_name", EXTRACT_STRUCTURED_VALUES_TEST_CASES)
def test_extract_structured_values(case_name):
    sample, era, expected = EXTRACT_STRUCTURED_VALUES_TEST_CASES[case_name]

    actual = extract_structured_values(sample, era)

    print("\n", actual, "\n")

    assert actual == expected, f"\nInput record:\n{sample}"


# Part-of from parenthesis
@pytest.mark.parametrize(
    "record, era, expected",
    [
        pytest.param(
            "isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287.) Stockholm",
            "parenthesized",
            (
                "isborgs slott.  Stockholm",
                "Antikvariska studier. 4. Sthlm 1950, s. 221-287.",
            ),
            id="simple-parenthesized-partof",
        ),
        pytest.param(
            "isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. (VHAAH 71.))",
            "parenthesized",
            (
                "isborgs slott.",
                "Antikvariska studier. 4. Sthlm 1950, s. 221-287. (VHAAH 71.)",
            ),
            id="nested-parentheses-inside-partof",
        ),
        pytest.param(
            "isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. VHAAH 71.))",
            "parenthesized",
            (
                "isborgs slott. (Antikvariska studier. 4. Sthlm 1950, s. 221-287. VHAAH 71.))",
                None,
            ),
            id="unbalanced-parentheses-not-extracted",
        ),
        pytest.param(
            "isborgs slott.",
            "parenthesized",
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

    result = extract_structured_values(record["sample"], False)

    assert result[0] is None
    assert result[1] == "Slott, Svenska, och herresäten vid 1900-talets början."


@pytest.mark.xfail(
    reason="If removing a year that has been confused with extent leads to the extent just being 's', give up on extrating the extent."
)
def test_extent_dot_year(note, expected):
    note = "524, (2) s. 1951."
    result = extract_extent(note, False)
    assert result == ""
