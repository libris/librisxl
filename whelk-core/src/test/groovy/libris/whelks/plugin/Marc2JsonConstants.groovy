package se.kb.libris.whelks.plugin

interface Marc2JsonConstants {
    final static String RAW_LABEL = "marc21"
    static def AUTHOR_MARC_0 = ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"]]]
    static def AUTHOR_LD_0   = ["preferredNameForThePerson" : "Svensson, Sven", "surname":"Svensson", "givenName":"Sven", "name": "Sven Svensson"]
    static def AUTHOR_MARC_1 = ["ind1":"0","ind2":" ","subfields":[["a": "E-type"]]]
    static def AUTHOR_LD_1   = ["preferredNameForThePerson" : "E-type", "name":"E-type"]
    static def AUTHOR_MARC_2 = ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["d": "1952-"]]]
    static def AUTHOR_LD_2  = ["preferredNameForThePerson" : "Svensson, Sven","surname":"Svensson", "givenName":"Sven", "name": "Sven Svensson", "dateOfBirth":["@type":"year","@value":"1952"]]
    static def AUTHOR_MARC_3 = ["ind1":"1","ind2":" ","subfields":[["a": "Nilsson, Nisse"], ["d": "1948-2010"]]]
    static def AUTHOR_LD_3   = ["preferredNameForThePerson" : "Nilsson, Nisse","surname":"Nilsson",
                                    "givenName":"Nisse", "name": "Nisse Nilsson",
                                    "dateOfBirth":["@type":"year","@value":"1948"],
                                    "dateOfDeath":["@type":"year","@value":"2010"]]
    static def AUTHOR_MULT_MARC_0 = ["fields":[["100":["ind1":"1","subfields":[["a": "Svensson, Sven"]]]],["700":["ind1":"1","subfields":[["a":"Karlsson, Karl,"]]]]]]
    static def AUTHOR_MULT_LD_0 = ["authorList":[["preferredNameForThePerson" : "Svensson, Sven", "surname":"Svensson", "givenName":"Sven", "name": "Sven Svensson"],["preferredNameForThePerson" : "Karlsson, Karl", "surname":"Karlsson", "givenName":"Karl", "name": "Karl Karlsson"]]]

    static def AUTHOR_MARC_4 = ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]
    //static def AUTHOR_LD_4   = [(RAW_LABEL):["fields":[["100":["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]]]]]
    static def AUTHOR_LD_4   = false

    static def TITLE_MARC_0 = [ "ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel"], ["c": "Kalle Kula"]]]
    static def TITLE_LD_0 = ["titleProper" : "Bokens titel", "statementOfResponsibilityRelatingToTitleProper" : "Kalle Kula"]
    static def TITLE_MARC_1 = [ "ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel"], ["c": "Kalle Kula"],["z":"foo"]]]
    //static def TITLE_LD_1 = [(RAW_LABEL):["fields":[["245":[ "ind1":" ", "ind2": " ", "subfields":[["a":"Bokens titel"], ["c": "Kalle Kula"], ["z":"foo"]]]]]]]
    static def TITLE_LD_1 = false

    static def ISBN_MARC_0 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6 (inb.)"]]]
    static def ISBN_LD_0 = ["isbn":"9100563226", "isbnRemainder": "(inb.)"]
    static def ISBN_MARC_1 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"]]]
    static def ISBN_LD_1 =  ["isbn":"9100563226"]
    static def ISBN_MARC_2 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6 (inb.)"], ["c":"310:00"]]]
    static def ISBN_LD_2 = ["isbn":"9100563226", "isbnRemainder": "(inb.)", "termsOfAvailability":["literal":"310:00"]]
    static def ISBN_MARC_3 = ["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"], ["z":"foo"]]]
    //static def ISBN_LD_3 = [(RAW_LABEL):["fields":[["020":["ind1":" ","ind2":" ", "subfields":[["a": "91-0-056322-6"], ["z":"foo"]]]]]]]
    static def ISBN_LD_3 = false
    static def CLEANED_ISBN_MARC_0 = ["ind1":" ","ind2":" ", "subfields":[["a": "9100563226 (inb.)"]]]
    static def CLEANED_ISBN_MARC_1 = ["ind1":" ","ind2":" ", "subfields":[["a": "9100563226"]]]
    static def CLEANED_ISBN_MARC_2 = ["ind1":" ","ind2":" ", "subfields":[["a": "9100563226 (inb.)"], ["c":"310:00"]]]
    static def CLEANED_ISBN_MARC_3 = ["ind1":" ","ind2":" ", "subfields":[["a": "9100563226"], ["z":"foo"]]]

    static def PUBLISHER_MARC_0 = ["ind1":" ", "ind2": " ", "subfields":[["a": "Stockholm :", "b":"Bonnier,", "c":"1996 ;", "d":"(Finland)"]]]
    static def PUBLISHER_LD_0   = ["placeOfPublication":["label":"Stockholm"], "publisherName":"Bonnier", "dateOfPublication":["@type":"year","@value":"1996"], "placeOfManufacture":["label":"Finland"]]
    static def BIBLIOGRAPHY_MARC_0 = ["ind1":" ","ind2":" ","subfields":[["9":"BULB"],["9":"SEE"],["9":"KVIN"]]]
    static def BIBLIOGRAPHY_LD_0 = ["marc:bibliographyCode":["BULB","SEE","KVIN"]]
    static def CTRLNR_MARC_0 = "123456"
    static def CTRLNR_LD_0 = ["controlNumber":"123456"]
    static def TIMESTAMP_MARC_0 = "20040809152032.0"
    static def TIMESTAMP_LD_0 = ["dateAndTimeOfLatestTransaction":"2004-08-09T15:20:32.0"]
}
