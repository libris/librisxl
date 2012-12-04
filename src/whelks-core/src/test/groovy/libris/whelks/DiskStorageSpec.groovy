package se.kb.libris.whelks.component

import spock.lang.Specification

class DiskStorageSpec extends Specification {

    final static String TMP_STORAGE = "/tmp/teststorage"
    final static String DOC_FOLDER = "_"
    def disk = new DiskStorage(TMP_STORAGE)


    def "should build proper path"() {
        expect:
            disk.buildPath(new URI(uri), false) == path
        where:
            uri                       | path
            "/bib/123"                | TMP_STORAGE+"/bib/" + DOC_FOLDER + "/123"
            "/bib/12345678"           | TMP_STORAGE+"/bib/1234/" + DOC_FOLDER + "/12345678"
            "/bib/name/12345678"      | TMP_STORAGE+"/bib/name/1234/" + DOC_FOLDER + "/12345678"
            "/bib/123456789"          | TMP_STORAGE+"/bib/1234/5678/" + DOC_FOLDER + "/123456789"
            "/bib/some/other/path/1"  | TMP_STORAGE+"/bib/some/other/path/" + DOC_FOLDER + "/1"
            "/bib/123.json"           | TMP_STORAGE+"/bib/123/" + DOC_FOLDER + "/123.json"
            "/bib"                    | TMP_STORAGE+"/"+DOC_FOLDER+"/bib"
            "/documents"              | TMP_STORAGE+"/docu/ment/"+DOC_FOLDER+"/documents"
            "/1234567890abcdefghijkl" | TMP_STORAGE+"/1234/5678/90ab/cdef/ghij/"+DOC_FOLDER+"/1234567890abcdefghijkl"
    }


}
