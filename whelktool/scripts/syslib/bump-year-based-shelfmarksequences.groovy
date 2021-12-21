/**
 * This script bumps the year for "year based" ShelfMarkSequences
 * 
 * Any ShelfMarkSequence with YY or YYYY in the label (e.g. "Sv2021") will be replacedBy
 * a new sequence starting at 1 and with the new year in the label (e.g. "Sv2022").
 * 
 * To be scheduled at New Year's Eve 🎆
 */

import java.time.LocalDate
import java.time.Month

report = getReportWriter("report.txt")

def now = LocalDate.now()

if (now.month != Month.DECEMBER && now.month != Month.JANUARY) {
    throw new RuntimeException("Can only run this script in December or January")
}

def oldYear = now.month == Month.DECEMBER ? now.year : now.year - 1
def newYear = oldYear + 1

def oldFull = "$oldYear".toString()
def newFull = "$newYear".toString()

def oldShort = "${oldYear % 100}".toString()
def newShort = "${newYear % 100}".toString()

selectByIds(queryIds(["@type": ["ShelfMarkSequence"]]).collect()) { s ->
    Map thing = s.graph[1]
    
    if (thing.shelfMarkStatus != "ActiveShelfMark") {
        return
    }
    
    def label = thing.label 
    if (label instanceof String) {
        if (label.contains(oldFull)) {
            replaceShelfMarkSequence(s, label.replace(oldFull, newFull))
        }
        else if (label.contains(oldShort)) {
            replaceShelfMarkSequence(s, label.replace(oldShort, newShort))
        }
    } 
}

void replaceShelfMarkSequence(old, String newLabel) {
    def newItem = create(old.doc.clone().data)

    newItem.graph[1]['label'] = newLabel
    newItem.graph[1]['nextShelfControlNumber'] = 1

    selectFromIterable([newItem], { it.scheduleSave() })
    selectByIds([newItem.doc.shortId], {
        // ShelfMarkSequences are access controlled on meta.descriptionCreator.
        // But we cannot set that while creating, update it afterwards instead.
        it.graph[0].descriptionCreator = old.graph[0].descriptionCreator 
        it.scheduleSave()
    })

    old.graph[1]['replacedBy'] = ['@id': newItem.doc.getThingIdentifiers().first()]
    old.graph[1]['shelfMarkStatus'] = "InactiveShelfMark"
    
    old.scheduleSave()
    
    report.println("${old.graph[1]['label']} -> $newLabel ($old.doc.shortId -> $newItem.doc.shortId)")
}
