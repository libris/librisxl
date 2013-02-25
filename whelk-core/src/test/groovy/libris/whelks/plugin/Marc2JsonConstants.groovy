package se.kb.libris.whelks.plugin

interface Marc2JsonConstants {
    final static String RAW_LABEL = "marc21"
    static def AUTHOR_MARC_0 = ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"]]]
    static def AUTHOR_LD_0   = ["@type":"Person","authoritativeName" : "Svensson, Sven", "familyName":"Svensson", "givenName":"Sven", "name": "Sven Svensson"]
    static def AUTHOR_MARC_1 = ["ind1":"0","ind2":" ","subfields":[["a": "E-type"],["4":"mus"]]]
    static def AUTHOR_LD_1   = ["@type":"Person","authoritativeName" : "E-type", "name":"E-type"]
    static def AUTHOR_MARC_2 = ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["d": "1952-"]]]
    static def AUTHOR_LD_2  = ["@type":"Person","authoritativeName" : "Svensson, Sven","familyName":"Svensson", "givenName":"Sven", "name": "Sven Svensson", "birthYear":"1952","authorizedAccessPoint":"Svensson, Sven, 1952-"]
    static def AUTHOR_MARC_3 = ["ind1":"1","ind2":" ","subfields":[["a": "Nilsson, Nisse"], ["d": "1948-2010"]]]
    static def AUTHOR_LD_3   = ["@type":"Person","authoritativeName" : "Nilsson, Nisse","familyName":"Nilsson",
                                    "givenName":"Nisse", "name": "Nisse Nilsson",
                                    "birthYear":"1948",
                                    "deathYear":"2010","authorizedAccessPoint":"Nilsson, Nisse, 1948-2010"]
    static def AUTHOR_MULT_MARC_0 = ["fields":[["100":["ind1":"1","subfields":[["a": "Svensson, Sven"]]]],["700":["ind1":"1","subfields":[["a":"Karlsson, Karl,"]]]]]]
    static def AUTHOR_MULT_LD_0 = [["@type":"Person","authoritativeName" : "Svensson, Sven", "familyName":"Svensson", "givenName":"Sven", "name": "Sven Svensson"],["@type":"Person","authoritativeName" : "Karlsson, Karl", "familyName":"Karlsson", "givenName":"Karl", "name": "Karl Karlsson"]]
    static def AUTHOR_MULT_MARC_1 = ["fields":[["100":["ind1":"1","subfields":[["a": "Svensson, Sven"]]]],["700":["ind1":"1","subfields":[["a":"Karlsson, Karl,"],["4":"ill"]]]]]]
    static def AUTHOR_MULT_LD_1 = [["@type":"Person","authoritativeName" : "Svensson, Sven", "familyName":"Svensson", "givenName":"Sven", "name": "Sven Svensson"]]
    static def AUTHOR_MULT_LD_2 = [["@type":"Person","authoritativeName" : "Karlsson, Karl", "familyName":"Karlsson", "givenName":"Karl", "name": "Karl Karlsson"]]
    
    static def AUTHOR_MARC_6 = ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]
    static def AUTHOR_LD_6   = ["@type":"Person","authoritativeName" : "Nilsson, Nisse","familyName":"Nilsson",
                                    "givenName":"Nisse", "name": "Nisse Nilsson",
                                    "birthYear":"1948",
                                    "deathYear":"2010"]
    static def AUTHOR_MARC_4 = ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]
    static def AUTHOR_LD_5   = [(RAW_LABEL):["fields":[["100":["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]]]]]
    static def AUTHOR_LD_4   = false

    static def TITLE_MARC_0 = ["ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel"], ["c": "Kalle Kula"]]]
    static def TITLE_LD_0 = ["title": "Bokens titel", "statementOfResponsibility": "Kalle Kula"]
    static def TITLE_MARC_1 = ["ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel"], ["c": "Kalle Kula"],["z":"foo"]]]
    static def TITLE_LD_2 = [(RAW_LABEL):["fields":[["245":["ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel"], ["c": "Kalle Kula"], ["z":"foo"]]]]]]]
    static def TITLE_LD_1 = false

    static def ISBN_MARC_0 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6 (inb.)"]]]
    static def ISBN_LD_0 = ["isbn":"9100563226", "isbnData": "(inb.)"]
    static def ISBN_MARC_1 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"]]]
    static def ISBN_LD_1 =  ["isbn":"9100563226"]
    static def ISBN_MARC_2 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6 (inb.)"], ["c":"310:00"]]]
    static def ISBN_LD_2 = ["isbn":"9100563226", "isbnData": "(inb.)", "termsOfAvailability":["literal":"310:00"]]
    static def ISBN_MARC_3 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"], ["g":"foo"]]]
    static def ISBN_MARC_5 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"], ["z":"foo"]]]
    static def ISBN_LD_5 = ["isbn":"9100563226", "isbnData": "(inb.)", "termsOfAvailability":["literal":"310:00"], "deprecatedIsbn":"foo"]
    static def ISBN_LD_4 = [(RAW_LABEL):["fields":[["020":["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"], ["g":"foo"]]]]]]]
    static def ISBN_LD_3 = false
    static def CLEANED_ISBN_MARC_0 = ["ind1":" ","ind2":" ", "subfields":[["a": "9100563226 (inb.)"]]]
    static def CLEANED_ISBN_MARC_1 = ["ind1":" ","ind2":" ", "subfields":[["a": "9100563226"]]]
    static def CLEANED_ISBN_MARC_2 = ["ind1":" ","ind2":" ", "subfields":[["a": "9100563226 (inb.)"], ["c":"310:00"]]]
    static def CLEANED_ISBN_MARC_3 = ["ind1":" ","ind2":" ", "subfields":[["a": "9100563226"], ["z":"foo"]]]

    static def PUBLISHER_MARC_0 = ["ind1":" ", "ind2": " ", "subfields":[["a": "Stockholm :", "b":"Bonnier,", "c":"1996 ;", "e":"(Finland)"]]]
    static def PUBLISHER_MARC_1 = ["ind1":" ", "ind2": " ", "subfields":[["a": "Stockholm"], ["b":"Bonnier"], ["c":"1996"], ["e":"Finland"]]]
    static def PUBLISHER_LD_0   = ["placeOfPublication":["label":"Stockholm"], "publisherName":"Bonnier", "dateOfPublication":["@type":"year","@value":"1996"], "placeOfManufacture":["label":"Finland"]]
    static def BIBLIOGRAPHY_MARC_0 = ["ind1":" ","ind2":" ","subfields":[["9":"BULB"],["9":"SEE"],["9":"KVIN"]]]
    static def BIBLIOGRAPHY_LD_0 = ["marc:bibliographyCode":["BULB","SEE","KVIN"]]
    static def CTRLNR_MARC_0 = "123456"
    static def CTRLNR_LD_0 = ["controlNumber":"123456"]
    static def TIMESTAMP_MARC_0 = new Date(1092057632000)
    static def TIMESTAMP_LD_0 = ["dateAndTimeOfLatestTransaction":"2004-08-09T15:20:32.0"]

    static def OTH_IDENT_MARC_1 = ["ind1":"7","subfields":[["a":"1234","2":"mySpecialIdentifier"]]]
    static def OTH_IDENT_MARC_2 = ["ind1":"1","subfields":[["a":"1234"]]]
    static def OTH_IDENT_LD_1 =   ["identifier":["@type":"Identifier", "identifierScheme":"mySpecialIdentifier","identifierValue":"1234"]]
    static def OTH_IDENT_LD_2 =   ["upc":"1234"]
}
