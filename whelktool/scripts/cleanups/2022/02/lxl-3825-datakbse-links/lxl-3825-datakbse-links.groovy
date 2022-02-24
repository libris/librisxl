/*
Replace associatedMedia links to data.kb.se

See LXL-3825 for more information

cat id_old.tsv | cut -f1 | while read ID; do echo $ID $(curl -s -X 'GET' -H 'accept: application/json' "https://data.kb.se/search/?q=$ID&limit=20&offset=0" | jq '.hits | .[] | .hasFilePackage | ."@id"' | tr -d '"' | tr '\n' '\t') ; done > id_new.txt

cat id_new.txt | grep -E "^[0-9a-z]+\s*$" > bad_ids.txt

join -t $'\t' -j 1 -o 2.1,2.2,1.2 id_old.sorted.tsv id_new.sorted.tsv | > id_new_old.tsv
*/

import whelk.util.DocumentUtil

INPUT_FILE_NAME = 'id_new_old.tsv'

notModified = getReportWriter("not-modified.txt")

Map<String, Map> map = [:]

new File(scriptDir, INPUT_FILE_NAME).readLines().each {line ->
    def (id, newUri, oldUri) = line.split('\t')
    map[id] = [(oldUri): newUri]
}

selectByIds(map.keySet()) { bib ->
    map[bib.doc.shortId].each { before, after ->
        boolean modified = DocumentUtil.traverse(bib.graph) { value, path -> 
            if (value == before){
                return new DocumentUtil.Replace(after)
            }
        }
        
        if (modified) {
            bib.scheduleSave()
        }
        else {
            notModified.println("$bib.doc.shortId")
        }
    }
}