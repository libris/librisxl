PrintWriter failedAuthIDs = getReportWriter("failed-authIDs")
PrintWriter scheduledForUpdating = getReportWriter("scheduled-updates")

def listofIds = []
def isnimap = [:]

new File(scriptDir, "isni_numbers.txt").each{ row -> 

def (ID, ISNI) = row.split(/\t/).collect { it.trim() } // hämta id och nummer, separera på tab

listofIds.add(ID)
isnimap[ID] = ISNI 
}

def ids = selectByIds(listofIds) { auth ->

def aukt = auth.graph[1]
def uri = auth.graph[0]["@id"]

id = uri.split('/')[3] // ta ut id från uri

def ISNI = isnimap[id] // matcha ISNI-nummer mot id

aukt["identifiedBy"]?.removeIf { it["typeNote"]?.contains("isni") || false } // radera element i identifiedBy som har ISNI-nummer (ofta felaktiga - dessa kan iofs vara korrekta, men läses in på nytt i senare steg))
aukt["identifiedBy"]?.removeIf { it["@type"] == 'ISNI' || false }
        
if (!aukt["identifiedBy"]) { // lägg till identifiedBy om det saknas

        aukt["identifiedBy"] = []

        }

    aukt["identifiedBy"] << // lägg till isni-nr: "@type": "Identifier","typeNote": "isni", "value": "[ISNI-nummer]
        [   
            "@type": "Identifier",
          "typeNote": "isni",
          "value": ISNI
        ]

scheduledForUpdating.println("${auth.doc.getURI()}")
        auth.scheduleSave(loud: true, onError: { e ->
            failedAuthIDs.println("Failed to update ${auth.doc.shortId} due to: $e")
        })
}
