package se.kb.libris.whelks.plugin

interface Marc2JsonConstants {
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
    static def AUTHOR_MARC_4 = ["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]
    static def AUTHOR_LD_4   = [(Marc2JsonLDConverter.RAW_LABEL):["100":["ind1":"1","ind2":" ","subfields":[["a": "Svensson, Sven"], ["z": "foo"]]]]]
}
