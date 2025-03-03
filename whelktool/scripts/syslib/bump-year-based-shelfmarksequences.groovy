/**
 * This script bumps the year for "year based" ShelfMarkSequences
 * 
 * Any ShelfMarkSequence with YY or YYYY in the label/qualifier (e.g. "Sv2021") will be replacedBy
 * a new sequence starting at 1 and with the new year in the label/qualifier (e.g. "Sv2022").
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

    def modified = false
    def newItem = create(s.doc.clone().data)
    def label = thing.label

    if (label instanceof String) {
        if (label.contains(oldFull)) {
            modified = replaceShelfMarkSequence(s, newItem, label.replace(oldFull, newFull), 'label')
        } else if (label.contains(oldShort)) {
            modified = replaceShelfMarkSequence(s, newItem, label.replace(oldShort, newShort), 'label')
        }
    }

    def qualifier = thing.qualifier
    def newQualifier = asList(qualifier).findResults {
        if (it instanceof String) {
            if (it.contains(oldFull)) {
                return it.replace(oldFull, newFull)
            } else if (it.contains(oldShort)) {
                return it.replace(oldShort, newShort)
            } else { // NOP
                return it
            }
        }
    }

    newQualifier = qualifier instanceof String ?  newQualifier.pop() : newQualifier
    if (newQualifier && newQualifier != qualifier) {
        modified = replaceShelfMarkSequence(s, newItem, newQualifier, 'qualifier')
    }

    if (modified) {
        createNewAndUpdateOld(s, newItem)
    }
}

boolean replaceShelfMarkSequence(old, newItem, newLabel, String prop) {
    newItem.graph[1][prop] = newLabel
    newItem.graph[1]['nextShelfControlNumber'] = 1

    report.println("${old.graph[1][prop]} -> $newLabel ($old.doc.shortId -> $newItem.doc.shortId)")
    return true
}

void createNewAndUpdateOld(old, newItem) {
    selectFromIterable([newItem], { it.scheduleSave(loud: true) })
    selectByIds([newItem.doc.shortId], {
        // ShelfMarkSequences are access controlled on meta.descriptionCreator.
        // But we cannot set that while creating, update it afterwards instead.
        it.graph[0].descriptionCreator = old.graph[0].descriptionCreator
        it.scheduleSave()
    })

    old.graph[1]['replacedBy'] = ['@id': newItem.doc.getThingIdentifiers().first()]
    old.graph[1]['shelfMarkStatus'] = "InactiveShelfMark"

    old.scheduleSave()
}