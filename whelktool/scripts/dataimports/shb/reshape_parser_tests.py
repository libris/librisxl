import pytest
from reshape_shb import (
    extract_extent,
    extract_partof_from_parenthesis,
    parse_note,
    parse_note,
)

### End-to-end parsing ###

# TODO or not todo?
# Parse out series/partOf info for older descriptions (no parenthesis or "I:" to go by)
# Identify contributions (bidrag) when there is no extent to go by


def test_parse_note_1771_1874_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. Publ-titel YYYY, nr (DD/MM).",
        "sample": "Holm, C. J., Också några ord om slaget vid Porosalmiden 12 juni 1789. Svenska Tidningen 1853, N:o 72 (81/8),",
    }

    result = parse_note(record["sample"], "early")
    
    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Holm, C. J."
    assert result["title"] == "Också några ord om slaget vid Porosalmiden 12 juni 1789"
    assert result["host"] == {
        "title": "Svenska Tidningen",
        "part_number": "1853, N:o 72 (81/8)",
    }


def test_parse_note_1875_1900_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. Publ-titel YYYY, nr.",
        "sample": "Hagemann, A., Et historisk Minde i Höifjeldet. [1657.] Aftenposten (Kristiania) 1897, N:r 186.",
    }

    result = parse_note(record["sample"], "early")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Hagemann, A."
    assert result["title"] == "Et historisk Minde i Höifjeldet"
    assert result["subtitle"] == "[1657.]"
    assert result["host"] == {
        "title": "Aftenposten (Kristiania)",
        "part_number": "1897, N:r 186",
    }


def test_parse_note_1936_1950_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. (Publ-titel DD/MM YYYY.)",
        "sample": "Ahnlund, N., Rikskanslerns titlar. [Axel Oxenstierna.] (SvD 14/7 1939.)",
    }

    result = parse_note(record["sample"], "parenthesized")
    print(result)
    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ahnlund, N."
    assert result["title"] == "Rikskanslerns titlar"
    assert result["subtitle"] == "[Axel Oxenstierna.]"
    assert result["host"] == {"title": "SvD", "part_number": "14/7 1939"}


def test_parse_note_1936_1950_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. (Publ-titel nr (årtal), sid.)",
        "sample": "Walde, O., Bielkeättens insatser i svensk bibliofili. Med särskild hänsyn till Bielkebiblioteket på Skokloster. (NTBB 27 (1940), s. 1-45.)",
    }

    result = parse_note(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Walde, O."
    assert result["title"] == "Bielkeättens insatser i svensk bibliofili"
    assert result["subtitle"] == "Med särskild hänsyn till Bielkebiblioteket på Skokloster"
    assert result["host"] == {
        "title": "NTBB",
        "part_number": "27 (1940)",
        "extent": "s. 1-45",
    }


def test_parse_note_1936_1950_Monografi():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)",
        "sample": "Weibull, C, Händelser och utvecklingslinjer. Historiska studier. Lund 1949,  254 s. (Göteborgs högskola. Forskningar och föreläsningar.) Rec. i HT 70 (1950), s. 69-70 av T. [T:son] H[öjer]; i SvD 21/11 1949 av  dens.; i SDS 25/11 1949 av K.-E. L[öfqvist]; i FT 148 (1950), s. 59-61 av 0.  M[usteli]n; i StT 1/12 1949 av S. U. Palme; i GHT 7/12 1949 av K[nut] P[etersson]. ",
    }

    result = parse_note(record["sample"], "parenthesized")
    print(result)
    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Weibull, C"
    assert result["title"] == "Händelser och utvecklingslinjer"
    assert result["subtitle"] == "Historiska studier"
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


def test_parse_note_1901_1920_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. Publ-titel YYYY, numrering (DD/MM).",
        "sample": "Stridsberg, G., En svensk kulturbild. Sveriges historia och kammararkivets traditioner. Svenska Dagbladet 1908, N:o 127 (n/6).",
    }
    result = parse_note(record["sample"], "early")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Stridsberg, G."
    assert result["title"] == "En svensk kulturbild"
    assert result["subtitle"] == "Sveriges historia och kammararkivets traditioner"
    assert result["host"] == {
        "title": "Svenska Dagbladet",
        "part_number": "1908, N:o 127 (n/6)",
    }

def test_parse_note_1771_1874_1875_1900_1901_1920_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn [initial]., Titel. undertitel. Publ-titel. nr, sid.",
        "sample": "Staven OW, L., Om förhållandet mellan politisk historia och kultur- historia. Hist, Tidskr. 1895, s. 415-430.",
    }

    result = parse_note(record["sample"], "early")
    print(result)

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Staven OW, L."
    assert (
        result["title"]
        == "Om förhållandet mellan politisk historia och kultur- historia"
    )
    assert result["host"] == {"title": "Hist, Tidskr", "part_number": "1895"}
    assert result["extent"] == "s. 415-430"

# TODO Try extracting period-delimited (non-journal) host publication/series 
def test_parse_note_1771_1874_1875_1900_1901_1920_Monografi():
    record = {
        "key": "Efternamn, Förnamn [initial]., Titel. undertitel. sid. Ort år. Serietillhörighet. numrering.",
        "sample": "Silfverstolpe, C, Klosterfolket i Vadstena. Personhistoriska anteck- ningar. H. 1-2. 167+ (1) s. +1 pl. Sthlm 1898, 99. Skrifter och handlingar utgifna genom Svenska Autografsällskapet. 4.",
    }
    result = parse_note(record["sample"], "early")

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Silfverstolpe, C"
    assert result["extent"] == "167+ (1) s. +1 pl."
    assert result["title"] == "Klosterfolket i Vadstena"
    assert result["subtitle"] == "Personhistoriska anteck- ningar. H. 1-2"
    assert result["place"] == "Sthlm"
    assert result["year"] == "1898, 99"
    assert result["host"] == {"title": "Skrifter och handlingar utgifna genom Svenska Autografsällskapet", "part_number": "4"}

def test_parse_note_1971_1975_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel DD.MM YYYY",
        "sample": "Bramstång, Mats, Glimtar av ett helgon [Birgitta]. - I: HbgD 6.10 1073",
    }
    result = parse_note(record["sample"], "isbd_transition")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Bramstång, Mats"
    assert result["title"] == "Glimtar av ett helgon [Birgitta]"
    assert result["host"] == {"title": "HbgD", "part_number": "6.10 1073"}


def test_parse_note_1976_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel DD.MM.YYYY",
        "sample": "Palm, Sixten, Apropå slaget vid Lund. - I: SDS 14.3.1976",
    }
    result = parse_note(record["sample"], "isbd")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Palm, Sixten"
    assert result["title"] == "Apropå slaget vid Lund"
    assert result["host"] == {"title": "SDS", "part_number": "14.3.1976"}


def test_parse_note_1971_1975_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel årg(årtal):numrering, sid.",
        "sample": "Jansson, Bror, Helsjön — en berömd kuranstalt vid Västgötagränsen. — I: Vår bygd 56(1973), s. 49-59 : ill.",
    }
    result = parse_note(record["sample"], "isbd_transition")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Jansson, Bror"
    assert result["title"] == "Helsjön - en berömd kuranstalt vid Västgötagränsen"
    assert result["host"] == {
        "title": "Vår bygd",
        "part_number": "56(1973)",
        "extent": "s. 49-59 : ill.",
    }


def test_parse_note_1976_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. - I: Publ-titel, ISSN, numrering, årg, sid.",
        "sample": "Seitz, Heribert, Ett hundraårigt beslut : hur Armémuseumgrundades. - I: Meddelande / Armémuseum, ISSN 0349-1048, 37,1976/1977, s. 117-123",
    }
    result = parse_note(record["sample"], "isbd")

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


def test_parse_note_1976_Monografi():
    record = {
        "sample": "Larsson, Gunilla, Curt Weibull : en bibliografi 19 augusti 1976. -Stockholm, 1976. - 27 s. - (Acta Bibliothecae regiae Stockholmiensis,ISSN 0065-1060 ; 28)"
    }
    result = parse_note(record["sample"], "isbd")
    print(result)
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


def test_parse_note_1971_1975_Monografi():
    record = {
        "pattern": "Efternamn, Förnamn, Titel : undertitel. Ort år. sid. - (Serietillhörighet ; numrering)",
        "sample": "WeibuII, Curt, Die Geaten des Beowulfepos und Die dänischen Trelle-burgen : zwei Diskussionsbeiträge. Göteborg 1974. 43 s. - (Acta RegiaeSocietatis scientiarum et litterarum Gothoburgensis. Humaniora ; 10)",
    }
    result = parse_note(record["sample"], "isbd_transition")

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


def test_parse_note_1961_1970_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. - Publ-titel DD.MM YYYY.",
        "sample": "Jansson, Leonard, Ivar Axelsson Tott. - ÖresP 2.6 1962.",
    }

    result = parse_note(record["sample"], "dash_style")
    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Jansson, Leonard"
    assert result["title"] == "Ivar Axelsson Tott"
    assert result["host"] == {"title": "ÖresP", "part_number": "2.6 1962"}


def test_parse_note_1961_1970_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. - Publ-titel årg (årtal):numrering, sid.",
        "sample": "Lagerroth, Fredrik, Positiv rätt eller naturrätt? Ett statsrattsligt dilemma från svenskt 1700-tal. [Zusammenfassung.] - Scandia 33 (1967), s. 270-312.",
    }
    result = parse_note(record["sample"], "dash_style")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Lagerroth, Fredrik"
    assert result["title"] == "Positiv rätt eller naturrätt? Ett statsrattsligt dilemma från svenskt 1700-tal"
    assert result["subtitle"] == "[Zusammenfassung.]"
    assert result["host"] == {
        "title": "Scandia",
        "part_number": "33 (1967)",
        "extent": "s. 270-312",
    }


def test_parse_note_1951_1960_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel DD/MM YYYY.)",
        "sample": "Ennermark, S., Brasks boktryckare en pionjär i Malmö. (SDS 31/8 1952.)",
    }
    result = parse_note(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Ennermark, S."
    assert result["title"] == "Brasks boktryckare en pionjär i Malmö"
    assert result["host"] == {"title": "SDS", "part_number": "31/8 1952"}


def test_parse_note_1921_1935_Bidrag_tidningsartikel():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel YYYY: DD/MM.)",
        "sample": "Steckzén, B., Med svärdet och plogen. (SvD 1935: 2/9.)",
    }
    result = parse_note(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Steckzén, B."
    assert result["title"] == "Med svärdet och plogen"
    assert result["host"] == {"title": "SvD", "part_number": "1935: 2/9"}


def test_parse_note_1921_1935_1951_1960_Bidrag():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. (Publ-titel nr (årtal), sid.)",
        "sample": "Berg, Gösta, Svensk folklivskännedom. En översikt av de senare årenslitteratur. (Hävd och hembygd. 2 (1927), s. 76-95.)",
    }
    result = parse_note(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["primary_contributors"] == "Berg, Gösta"
    assert result["title"]== "Svensk folklivskännedom"
    assert result["subtitle"] == "En översikt av de senare årenslitteratur"
    assert result["host"] == {
        "title": "Hävd och hembygd",
        "part_number": "2 (1927)",
        "extent": "s. 76-95",
    }


def test_parse_note_1921_1935_Monografi():
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)",
        "sample": "Nohrstrom, Holger, Borgå gymnasiebibliotek och dess föregångare blandFinlands läroverksbibliotek. Ett bidrag till Finlands biblioteks- och kulturhistoria.Hfors 1927. (2), 291 s. (Helsingfors universitetsbiblioteks skrifter.10.)",
    }
    result = parse_note(record["sample"], "parenthesized")

    print(result)

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Nohrstrom, Holger"
    assert result["title"] == "Borgå gymnasiebibliotek och dess föregångare blandFinlands läroverksbibliotek"
    assert result["subtitle"] == "Ett bidrag till Finlands biblioteks- och kulturhistoria"
    assert result["place"] == "Hfors"
    assert result["year"] == "1927"
    assert result["extent"] == "(2), 291 s."
    assert result["host"] == {
        "title": "Helsingfors universitetsbiblioteks skrifter",
        "part_number": "10",
    }



def test_parse_note_1961_1970_Monografi():
    # TODO Missing colon before "Ill"?
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. Ort år. sid. - Serietillhörighet. numrering.",
        "sample": "Clemedson, Carl-Johan, Taxinge socken. Kultur, vegetation, flora. Nyköping 1970. 93 s. Ill. - Sörmländska handlingar. 27.",
    }
    result = parse_note(record["sample"], "dash_style")

    print(result)

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Clemedson, Carl-Johan"
    assert result["title"] == "Taxinge socken"
    assert result["subtitle"] == "Kultur, vegetation, flora"
    assert result["place"] == "Nyköping"
    assert result["year"] == "1970"
    assert result["extent"] == "93 s. Ill."
    assert result["host"] == {
        "title": "Sörmländska handlingar",
        "part_number": "27",
    }



def test_parse_note_1951_1960_Monografi():
    # TODO Issue with "pl.-bl."?
    record = {
        "pattern": "Efternamn, Förnamn, Titel. undertitel. sid. Ort år. (Serietillhörighet. numrering.)",
        "sample": "Davidsson, Åke, Handritade kartor över Finland i Uppsala universitetsbibliotek.  60 s., 3 pl.-bl. Uppsala 1957. (Acta Bibliothecae R. Universitatis Upsaliensis. 11.)",
    }
    result = parse_note(record["sample"], "parenthesized")

    print(result)

    assert result["is_component_part"] == False
    assert result["primary_contributors"] == "Davidsson, Åke"
    assert (
        result["title"]
        == "Handritade kartor över Finland i Uppsala universitetsbibliotek"
    )
    assert result["extent"] == "60 s., 3 pl.-bl."
    assert result["place"] == "Uppsala"
    assert result["year"] == "1957"
    assert result["host"] == {
        "title": "Acta Bibliothecae R. Universitatis Upsaliensis",
        "part_number": "11",
    }


@pytest.mark.xfail(
    reason="Don't know what to do with unusual combined extents / part numbers. Missing space before ',' does not make a difference."
)
def test_parse_note_1971_1975_Bidrag_utan_författare():
    record = {
        "pattern": "Titel : undertitel / Upphov. - I: Publ-titel årg(årtal):numrering, sid.",
        "sample": "Ystadsläkter under trenne sekel / sammanställningar av F. W. Grönwall.; komplettrade och utgivna av Sven Carlquist. — I: Ystadiana 19(1974),s. 73-123 ; 20(1975), s. 45-91.",
    }
    result = parse_note(record["sample"], "isbd_transition")

    print(result)

    assert result["is_component_part"] == True
    assert (
        result["other_contributors"]
        == "sammanställningar av F. W. Grönwall.; komplettrade och utgivna av Sven Carlquist."
    )
    assert result["title"] == "Ystadsläkter under trenne sekel"
    assert result["host"] == {
        "title": "Ystadiana",
        "part_number": "19(1974),s. 73-123 ; 20(1975), s. 45-91.",
    }


def test_parse_note_1976_Bidrag_utan_författare():
    record = {
        "pattern": "Titel : undertitel / Upphov. - I: Publ-titel, ISSN, numrering, årg, sid.",
        "sample": "Arbetare! Kamrater! : dokument kring första maj 1890 / LarsFrendel ... - I: Meddelanden från Arkivet för folkets historia, ISSN0345-7605, 4, 1976:2, s. 2-7",
    }
    result = parse_note(record["sample"], "isbd")

    assert result["is_component_part"] == True
    assert result["other_contributors"] == "LarsFrendel ..."
    assert result["title"] == "Arbetare! Kamrater!"
    assert result["subtitle"] == "dokument kring första maj 1890"
    assert result["host"] == {
        "title": "Meddelanden från Arkivet för folkets historia",
        "issn": "0345-7605",
        "part_number": "4, 1976:2",
        "extent": "s. 2-7",
    }


def test_parse_note_1976_Monografi_utan_författare():
    record = {
        "pattern": "Titel : undertitel / Upphov. - Ort, år. - sid.",
        "sample": "Folkhögskolan på Gotland 100 år : Gotlands läns folkhögskola ettsekel i folkbildningens tjänst : en berättelse / med bidrag av JohanAhlsten .... red.: Paul Norrby. - Visby, 1976. - 141 s. : ill.",
    }

    result = parse_note(record["sample"], "isbd")

    assert result["is_component_part"] == False
    assert (
        result["other_contributors"]
        == "med bidrag av JohanAhlsten .... red.: Paul Norrby"
    )
    assert result["title"] == "Folkhögskolan på Gotland 100 år"
    assert (
        result["subtitle"]
        == "Gotlands läns folkhögskola ettsekel i folkbildningens tjänst : en berättelse"
    )
    assert result["place"] == "Visby"
    assert result["year"] == "1976"
    assert result["extent"] == "141 s. : ill."


def test_parse_note_1971_1975_Monografi_utan_författare():
    record = {
        "pattern": "Titel : undertitel / Upphov. Ort år. sid. - (Serietillhörighet ; numrering)",
        "sample": "Polens krig med Sverige 1655-1660 : krigshistoriska studier / red.: ArneStade o. Jan Wimmer. Stockholm, 1973. 432 s. + 1 kartbl. : ill. (Carl XGustaf-studier ; 5). — Summary: A review of operations in the Polish-Swedish war of 1655-1660",
    }
    result = parse_note(record["sample"], "isbd_transition")

    print(result)

    assert result["is_component_part"] == False
    assert result["other_contributors"] == "red.: ArneStade o. Jan Wimmer"
    assert result["title"] == "Polens krig med Sverige 1655-1660"
    assert result["subtitle"] == "krigshistoriska studier"
    assert result["place"] == "Stockholm"
    assert result["year"] == "1973"
    assert result["extent"] == "432 s. + 1 kartbl. : ill."
    assert (
        result["remaining_note"]
        == "Summary: A review of operations in the Polish-Swedish war of 1655-1660"
    )
    assert result["host"] == {"title": "Carl XGustaf-studier", "part_number": "5"}


def test_parse_note_1961_1970_Bidrag_utan_författare():
    record = {
        "pattern": "Titel. - Publ-titel årg (årtal):numrering, sid.",
        "sample": "Arkivinventering i Södermanlands län 1958-1959. - Kommissionen för riksinven- tering av de enskilda arkiven. Bulletin 1 (1963), s. 5-18.",
    }

    result = parse_note(record["sample"], "dash_style")
    print(result)
    assert result["is_component_part"] == True
    assert result["title"] == "Arkivinventering i Södermanlands län 1958-1959"
    assert result["host"] == {
        "title": "Kommissionen för riksinven- tering av de enskilda arkiven. Bulletin",
        "part_number": "1 (1963)",
        "extent": "s. 5-18",
    }


def test_parse_note_1921_1935_1936_1950_Bidrag_utan_författare():
    record = {
        "pattern": "Titel. undertitel. (Publ-titel. nr. årg, sid.)",
        "sample": "Mo kyrka. Med anledning af dess hundraårsjubileum. Norgren, N., Gamlakyrkan. — Norbeck, O., Nya kyrkan. (Julhälsn. till församl. i ärkestiftet1922, s. 142-151.)",
    }

    result = parse_note(record["sample"], "parenthesized")

    assert result["is_component_part"] == True
    assert result["title"] == "Mo kyrka"
    assert result["subtitle"] == "Med anledning af dess hundraårsjubileum. Norgren, N., Gamlakyrkan. - Norbeck, O., Nya kyrkan"
    assert result["host"] == {
        "title": "Julhälsn. till församl. i ärkestiftet",
        "part_number": "1922",
        "extent": "s. 142-151",
    }


@pytest.mark.xfail(reason="Not sure what to do with 4:0 (probably misspelled quarto)")
def test_parse_note_1921_1935_1936_1950_Monografi_utan_författare():
    record = {
        "pattern": "Titel. undertitel. Ort år. sid. (Serietillhörighet. numrering.)",
        "sample": "Sveriges sjöfart. Sjöfartsväsendet, skeppsbyggeriet och handelsflottan. Enskildring under medverkan av fackmän utg. av Nils Gustaf Nilsson ochGustav Åsbrink med förord av K. A. Fryxell. Sthlm 1919-21. 4:0. xx,666 s. (4567 a)",
    }
    result = parse_note(record["sample"], "parenthesized")
    print(result)
    assert result["is_component_part"] == False
    assert result["title"] == "Sveriges sjöfart"
    assert result["subtitle"] == "Sjöfartsväsendet, skeppsbyggeriet och handelsflottan. Enskildring under medverkan av fackmän utg. av Nils Gustaf Nilsson ochGustav Åsbrink med förord av K. A. Fryxell"
    assert result["remaining_note"] == "4:0. (4567 a)"
    assert result["extent"] == "xx,666 s."
    assert result["place"] == "Sthlm"
    assert result["year"] == "1919-21"


@pytest.mark.xfail(
    reason="Fails because missing colon before 'Ill' excludes 'Ill' from extent"
)
def test_parse_note_1961_1970_Monografi_utan_författare():
    record = {
        "pattern": "Titel. undertitel. Ort år. sid. - Serietillhörighet. numrering.",
        "sample": "Magnus Erikssons landslag. I nusvensk tolkning av Åke Holmbäck och Elias Wes- sén. Sthlm 1962. lxix. 290 s., 6 pl.-bl. Ill. - Skrifter utg. av Inst. för rättshisto- risk forskning.",
    }
    result = parse_note(record["sample"], "dash_style")


    assert result["is_component_part"] == False
    assert (
        result["title"]
        == "Magnus Erikssons landslag. I nusvensk tolkning av Åke Holmbäck och Elias Wes- sén"
    )
    assert result["extent"] == "lxix. 290 s., 6 pl.-bl. Ill."
    assert (
        result["remaining_note"]
        == "Skrifter utg. av Inst. för rättshisto- risk forskning"
    )

@pytest.mark.xfail(
    reason="Beginning of series is not picked up due to missing space in '1623.Historiskt'"
)
def test_parse_note_1771_1874_1875_1900_1901_1920_Bidrag_utan_författare():
    record = {
        "pattern": "Titel. undertitel. Publ-titel. nr, sid.",
        "sample": "Diarium eller journal på hans maj:ts resa åt Danzig anno1623.Historiskt archivum (Loenbom). St. 2 (1774), s. 23-29.",
    }

    result = parse_note(record["sample"], "early")
    print(result)  
    assert result["is_component_part"] == True
    assert (
        result["title"]
        == "Diarium eller journal på hans maj:ts resa åt Danzig anno1623.Historiskt archivum (Loenbom)"
    )
    assert result["host"] == {"title": "St", "part_number": "2 (1774)"}
    assert result["extent"] == "s. 23-29"

# TODO Move "Red: " to other_contributors=
def test_parse_note_1951_1960_Monografi_utan_författare():
    record = {
        "pattern": "Titel. undertitel. sid. Ort år. (Serietillhörighet. numrering.)",
        "sample": "Företagsarkiven. Orientering om modern arkivorganisation. Red.: Jan Magnus  Fahlström. 233 s. Sthlm 1956. (Sv. arkivförb:s skriftser. 3.)",
    }
    result = parse_note(record["sample"], "parenthesized")

    print(result)

    assert result["is_component_part"] == False
    assert result["title"] == "Företagsarkiven"
    assert result["subtitle"] == "Orientering om modern arkivorganisation. Red.: Jan Magnus Fahlström"
    assert result["place"] == "Sthlm"
    assert result["year"] == "1956"
    assert result["host"] == {"title": "Sv. arkivförb:s skriftser", "part_number": "3"}


def test_parse_note_1771_1874_1875_1900_1901_1920_Monografi_utan_författare():
    record = {
        "pattern": "Titel. undertitel. sid. Ort år. Serietillhörighet. numrering.",
        "sample": "Aktstycker vedrörende Erik af Pommerns Afssettelse som Konge af Danmark, udgivne ved A. Hudt af Selskabet for Udgivelse af Kilder til dansk Historie. (2)+ 45 s. Khvn 1897.",
    }

    result = parse_note(record["sample"], "early")

    print(result)

    assert result["is_component_part"] == False
    assert (
        result["title"]
        == "Aktstycker vedrörende Erik af Pommerns Afssettelse som Konge af Danmark, udgivne ved A. Hudt af Selskabet for Udgivelse af Kilder til dansk Historie"
    )
    assert result["place"] == "Khvn"
    assert result["year"] == "1897"
    assert result["extent"] == "(2)+ 45 s."


### Parsing for specific values ###
# Main entity values
PARSE_NOTE_TEST_CASES = {
    "simple-author-with-initials": (
        "Surname, G.-N., Anything",
        "early",
        {
            "primary_contributors": "Surname, G.-N.",
            "title": "Anything",
            "is_component_part": True,
            "original_note": "Surname, G.-N., Anything"
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
            "original_note": "Schuck, A., H. Schücks enka & Co. AB 150 år. [Stockholm.] Sthlm 1947, 28 s."
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
            "original_note": "Meyerson, Å., Ett besök vid Stora Kopparberget och Sala gruva år 1662. (BBV 23 (1938), s. 325-343.)"
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
            "original_note": 'Davidsson, Åke, "En hoop Discantzböcker i godt förhwar..." : någotom Strängnäsgymnasiets musiksamling under 1600-talet. - I: Frånbiskop Rogge till Roggebiblioteket. Nyköping, 1976, s. 48-62'

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
            "original_note": "Barton, H. Arnold, A bibliography of writings in English by or onrecent Swedish emigration historians. - I: The Swedish pioneer, ISSN0039-7326, 27, 1976:3, s. 215-221"
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
            "remaining_note": "Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S. Stolpe; i DN 11.11.1977 av I. Algulin",
            "is_component_part": False,
            "original_note": "Jonsson, Inge, Swedenborg : sökaren i naturens och andens värld :hans verk och efterföljd / Inge Jonsson, Olle Hjern. -Stockholm, 1976. - 187 s. Rec. i SP 21.4.1977 av 6. Hillerdal; i NT-ÖD 29.4.1977 av S. Stolpe; i DN 11.11.1977 av I. Algulin"

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
            "original_note": "Fries, Elias, Hembygdsperiodika : förteckning över periodiskaskrifter samt skriftserier utgivna t.o.m. 1974 av hembygds- ochfornminnesföreningar samt länsmuseer m.fl. - Borås, 1976. - 40 bl. -(Specialarbete / Bibliotekshögskolan, ISSN 0347-1128 ; 1976:158)"

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
            "original_note": "Edvardsson, Lars, Kyrka och judendom : svensk judemission medsärskild hänsyn till Svenska israelmissionens verksamhet 1875-1975. -Lund, 1976. - 194 s. - (Bibliotheca historico-ecclesiasticaLundensis, ISSN 0346-5438 ; 6). - Diss. Hit deutscher ZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av I. Brohed"

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
            "original_note": "Frithz, Carl-Gösta, Till frågan om det s.k. Kelgeandshusmissaletsliturgihistoriska ställning. - Lund, 1976. - 428 s. - (Bibliothecatheologiae practicae, ISSN 0519-9859 ; 34) - Oiss. Mit deutscherZusammenfassungRec. i Kyrkohistorisk årsskrift 1976 av S. Helander"

        },
    ),
    "multiple-authors-and-complex-multi-part-extent": (
        "Erichsen, B., & Krarup, A., Dansk historisk Bibliografi. Bd 1-3. Khvn 1918-21, 1925-27, 1917. xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s.",
        "parenthesized",
        {
            "primary_contributors": "Erichsen, B., & Krarup, A.",
            "title": "Dansk historisk Bibliografi",
            "extent": "xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s.",
            "place": "Khvn",
            "year": "1918-21, 1925-27, 1917",
            "subtitle": "Bd 1-3",
            "is_component_part": False,
            "original_note": "Erichsen, B., & Krarup, A., Dansk historisk Bibliografi. Bd 1-3. Khvn 1918-21, 1925-27, 1917. xiii, (1), 794 s. + viii, 655 s. + (2), iv, 806, (1) s."

        },
    ),
}


@pytest.mark.parametrize("case_name", PARSE_NOTE_TEST_CASES)
def test_parse_note(case_name):
    sample, era, expected = PARSE_NOTE_TEST_CASES[case_name]

    actual = parse_note(sample, era)

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

    result = parse_note(record["sample"], False)

    assert result[0] is None
    assert result[1] == "Slott, Svenska, och herresäten vid 1900-talets början."


@pytest.mark.xfail(
    reason="If removing a year that has been confused with extent leads to the extent just being 's', give up on extrating the extent."
)
def test_extent_dot_year(note, expected):
    note = "524, (2) s. 1951."
    result = extract_extent(note, False)
    assert result == ""
