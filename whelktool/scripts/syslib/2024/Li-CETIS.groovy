File bibIDs = new File(scriptDir, "Li-CETIS-ID.txt")

selectByIds( bibIDs.readLines() ) { hold ->
def item = hold.graph[1]
def immAcq = [
                    "@type": "ImmediateAcquisition",
                    "marc:sourceOfAcquisition": "CETIS"
             ]
 
item['immediateAcquisition'] = (item['immediateAcquisition'] ?: []) + immAcq
    
hold.scheduleSave(loud: true)
	
}