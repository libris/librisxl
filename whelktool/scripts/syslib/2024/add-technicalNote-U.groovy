File bibIDs = new File(scriptDir, "uppsala-id.txt")

selectByIds( bibIDs.readLines() ) { bib ->

	def instance = bib.graph[0]
    
    if (instance["technicalNote"]) {
        instance["technicalNote"] <<
            [
             "@type": "TechnicalNote",
             "label": "Bevaras enligt TGV-plan/U"
             ]
    }    
    
    if (!instance["technicalNote"]) { 
            instance["technicalNote"] = []
            instance["technicalNote"] <<
            [
             "@type": "TechnicalNote",
             "label": "Bevaras enligt TGV-plan/U"
             ]
        }

    bib.scheduleSave(loud: true)
    }