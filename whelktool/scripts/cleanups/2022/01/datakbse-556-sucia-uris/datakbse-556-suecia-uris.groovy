/*
Update links to data.kb.se in associatedMedia.uri for Suecia docs

- Replace .tif links with .tif links
- Replace .zip links with data.kb.se landing page links 
- Remove .pdf links (dataset doesn't exist anymore)
- Also verify .tif file sizes in public note (they are all correct) 

See DATAKBSE-556 for more information

filesize.txt generated like this: 
cat new-tifs.txt | while read URI; do echo $URI $(curl -s -IXGET $URI | grep 'Content-Length' | grep -o [0-9]*); done > filesize.txt

 */
PDF_LINK_PREFIX = 'https://data.kb.se/datasets/2016/05/suecia/'

newTifs = getReportWriter("new-tifs.txt")
sizeDiff = getReportWriter("size-diff.txt")

Set<String> records = []
Map<String,String> replacementLinks = [:]
for (String line: new File(scriptDir, 'new_data.kb.se.tsv').readLines()) {
    def (recordId, oldLink, newUri) = line.split('\t')
    records.add(recordId)
    String newPkgUri = newUri.split('/')[0..-2].join('/') // drop last component 
    
    if (oldLink.endsWith('.tif')) {
        String filename = oldLink.split('/')[-1]
        String newLink = newPkgUri + '/' + filename
        newTifs.println(newLink)
        replacementLinks.put(oldLink, newLink)
    } else if (oldLink.endsWith('.zip')) {
        replacementLinks.put(oldLink, newPkgUri)
    }
}

Map<String, Integer> sizes = [:]
for (String line: new File(scriptDir, 'filesizes.txt').readLines()) {
    def (newUri, filesizeBytes) = line.split(' ')
    int fileSizeMb = Math.round(Integer.parseInt(filesizeBytes) / (1024 * 1024))
    sizes.put(newUri, fileSizeMb)
}

records.each { fix(it, replacementLinks, sizes) }

void fix(String id, Map<String,String> replacementLinks, Map<String, Integer> sizes) {
    if (id.isNumber()) {
        id = 'http://libris.kb.se/resource/bib/' + id
    }
    
    selectByIds([id]) { doc ->
        List associatedMedia = doc.graph[1].associatedMedia
        if (!associatedMedia) {
            return
        }
        
        def i = associatedMedia.iterator()
        while (i.hasNext()) {
            Map mediaObject = i.next()
            if (!mediaObject.uri) {
                continue
            }
            String oldUri = mediaObject.uri[0]
            
            if (oldUri.startsWith(PDF_LINK_PREFIX)) {
                i.remove()
                doc.scheduleSave(loud: true)
                continue
            }
            
            def newUri = replacementLinks[oldUri]
            if (newUri) {
                Integer size = sizes.get(newUri)
                if (size && !((String) mediaObject.'marc:publicNote'[0]).contains("$size MB")) {
                    sizeDiff.println("${doc.doc.shortId} ${mediaObject.'marc:publicNote'} $size")
                }

                mediaObject.uri[0] = newUri
                doc.scheduleSave(loud: true)
            }
        }
        
        // Some of these have associatedMedia
        String physical = doc.graph[1].reproductionOf?.'@id'
        if (physical) {
            fix(physical, replacementLinks, sizes)
        }
    }
}
