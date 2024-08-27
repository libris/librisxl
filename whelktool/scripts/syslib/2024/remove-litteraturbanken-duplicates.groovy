File littIDs = new File(scriptDir, "Del_litt_ID.txt")

List<String> ProgramLines = littIDs.readLines().drop(1)

for (String operation : ProgramLines) {
    String[] part = operation.split('\t')
    String bibID = part[0].trim()
    String holdID = part[1].trim()

    selectByIds([holdID])  { hold ->
        assert hold.doc.getHeldBySigel() == 'LITT'
        hold.scheduleDelete()
    }

    selectByIds([bibID])  { bib ->
        bib.scheduleDelete()
    }
}