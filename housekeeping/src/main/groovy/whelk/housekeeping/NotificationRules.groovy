package whelk.housekeeping

import whelk.Document

class NotificationRules {

    private static boolean personChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map) || !(agentAfter instanceof Map))
            return false

        if (agentBefore["@type"] == "Person" && agentAfter["@type"] == "Person") {
            if (
            agentBefore["familyName"] != agentAfter["familyName"] ||
                    agentBefore["givenName"] != agentAfter["givenName"] ||
                    agentBefore["name"] != agentAfter["name"] ||
                    (agentBefore["lifeSpan"] && agentBefore["lifeSpan"] != agentAfter["lifeSpan"]) // Change should trigger, add should not.
            )
                return true
        }
        return false
    }

    private static boolean organizationChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map) || !(agentAfter instanceof Map))
            return false

        if (agentBefore["@type"] == "Organization" && agentAfter["@type"] == "Organization") {
            if (
                    agentBefore["name"] != agentAfter["name"] ||
                            agentBefore["isPartOf"]["name"] != agentAfter["isPartOf"]["name"] ||
                            agentBefore["marc:subordinateUnit"] != agentAfter["marc:subordinateUnit"]
            )
                return true
        }
        return false
    }

    private static boolean meetingChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map) || !(agentAfter instanceof Map))
            return false

        if (agentBefore["@type"] == "Meeting" && agentAfter["@type"] == "Meeting") {
            if (
                    agentBefore["place"] != agentAfter["place"] ||
                            agentBefore["date"] != agentAfter["date"] ||
                            agentBefore["name"] != agentAfter["name"]
            )
                return true
        }
        return false
    }

    private static boolean jurisdictionChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map) || !(agentAfter instanceof Map))
            return false

        if (agentBefore["@type"] == "Jurisdiction" && agentAfter["@type"] == "Jurisdiction") {
            if (
                    agentBefore != agentAfter // For now
            )
                return true
        }
        return false
    }

    private static boolean familyChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map) || !(agentAfter instanceof Map))
            return false

        if (agentBefore["@type"] == "Family" && agentAfter["@type"] == "Family") {
            if (
                    agentBefore != agentAfter // For now
            )
                return true
        }
        return false
    }

    static Tuple primaryContributionChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object contributionsAfter = Document._get(["mainEntity", "instanceOf", "contribution"], instanceAfterChange.data)
        Object contributionsBefore = Document._get(["mainEntity", "instanceOf", "contribution"], instanceBeforeChange.data)
        if (contributionsBefore != null && contributionsAfter != null && contributionsBefore instanceof List && contributionsAfter instanceof List) {
            for (Object contrBefore : contributionsBefore) {
                for (Object contrAfter : contributionsAfter) {
                    if (contrBefore["@type"].equals("PrimaryContribution") && contrAfter["@type"].equals("PrimaryContribution")) {
                        if (contrBefore["agent"] != null && contrAfter["agent"] != null) {

                            if (
                                    personChanged(contrBefore["agent"], contrAfter["agent"]) ||
                                            meetingChanged(contrBefore["agent"], contrAfter["agent"]) ||
                                            organizationChanged(contrBefore["agent"], contrAfter["agent"])
                            ) {
                                    return new Tuple(true, contrBefore["agent"], contrAfter["agent"])
                            }

                        }
                    }
                }
            }
        }
        return new Tuple(false, null, null)
    }

    static Tuple subjectChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object subjectsAfter = Document._get(["mainEntity", "instanceOf", "subject"], instanceAfterChange.data)
        Object subjectsBefore = Document._get(["mainEntity", "instanceOf", "subject"], instanceBeforeChange.data)

        List removedSubjects = []
        List addedSubjects = []

        if (subjectsBefore != null && subjectsAfter != null && subjectsBefore instanceof List && subjectsAfter instanceof List) {

            // Find removed
            for (Object subjectBefore : subjectsBefore) {
                boolean subjectExistsAfter = false
                for (Object subjectAfter : subjectsAfter) {
                    if (
                            !personChanged(subjectBefore, subjectAfter) &&
                                    !meetingChanged(subjectBefore, subjectAfter) &&
                                    !jurisdictionChanged(subjectBefore, subjectAfter) &&
                                    !familyChanged(subjectBefore, subjectAfter) &&
                                    !organizationChanged(subjectBefore, subjectAfter)
                    ) {
                        subjectExistsAfter = true
                    }
                }
                if (!subjectExistsAfter) {
                    removedSubjects.add(subjectBefore)
                }
            }

            // Find added
            for (Object subjectAfter : subjectsAfter) {
                boolean subjectExistsBefore = false
                for (Object subjectBefore : subjectsBefore) {
                        if (
                            !personChanged(subjectBefore, subjectAfter) &&
                                    !meetingChanged(subjectBefore, subjectAfter) &&
                                    !jurisdictionChanged(subjectBefore, subjectAfter) &&
                                    !familyChanged(subjectBefore, subjectAfter) &&
                                    !organizationChanged(subjectBefore, subjectAfter)
                        ) {
                            subjectExistsBefore = true
                        }
                }
                if (!subjectExistsBefore) {
                    addedSubjects.add(subjectAfter)
                }
            }

            if (!addedSubjects.isEmpty() || !removedSubjects.isEmpty()) {
                return new Tuple(true, removedSubjects, addedSubjects)
            }

        }
        return new Tuple(false, null, null)
    }

    // NOT YET COMPLETE, CHECK ONLY THE TITLES
    static Tuple serialRelationChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        if (!Document._get(["mainEntity", "issuanceType"], instanceBeforeChange.data).equals("Serial"))
            return new Tuple(false, null, null)
        if (!Document._get(["mainEntity", "issuanceType"], instanceAfterChange.data).equals("Serial"))
            return new Tuple(false, null, null)

        Object continuedByBefore = Document._get(["mainEntity", "continuedBy"], instanceBeforeChange.data)
        Object continuedByAfter = Document._get(["mainEntity", "continuedBy"], instanceAfterChange.data)
        //System.err.println("COMPARING:\n\t" + continuedByAfter + "\n\t" + continuedByBefore)
        if (continuedByBefore != null && continuedByAfter != null && continuedByBefore instanceof List && continuedByAfter instanceof List) {
            if (continuedByAfter as Set != continuedByBefore as Set)
                return new Tuple(true, continuedByBefore, continuedByAfter)
        }

        Object continuesBefore = Document._get(["mainEntity", "continues"], instanceBeforeChange.data)
        Object continuesAfter = Document._get(["mainEntity", "continues"], instanceAfterChange.data)
        if (continuesBefore != null && continuesAfter != null && continuesBefore instanceof List && continuesAfter instanceof List) {
            if (continuesAfter as Set != continuesBefore as Set)
                return new Tuple(true, continuesBefore, continuesAfter)
        }

        return new Tuple(false, null, null)
    }

    static Tuple serialTerminationChanged(Document instanceBeforeChange, Document instanceAfterChange) {

        if (!Document._get(["mainEntity", "issuanceType"], instanceBeforeChange.data).equals("Serial"))
            return new Tuple(false, null, null)
        if (!Document._get(["mainEntity", "issuanceType"], instanceAfterChange.data).equals("Serial"))
            return new Tuple(false, null, null)

        Object publicationsBefore = Document._get(["mainEntity", "publication"], instanceBeforeChange.data)
        Object publicationsAfter = Document._get(["mainEntity", "publication"], instanceAfterChange.data)

        if (publicationsBefore instanceof List && publicationsAfter instanceof List &&
                publicationsBefore.size() == publicationsAfter.size()) {
            List beforeList = (List) publicationsBefore
            List afterList = (List) publicationsAfter
            for (int i = 0; i < beforeList.size(); ++i)
                if (!beforeList[i]["endYear"].equals(afterList[i]["endYear"])) {
                    return new Tuple(true, beforeList[i]["endYear"], afterList[i]["endYear"])
                }
        }

        return new Tuple(false, null, null)
    }

    static Tuple intendedAudienceChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object valueBefore = Document._get(["mainEntity", "instanceOf", "intendedAudience"], instanceBeforeChange.data)
        Object valueAfter = Document._get(["mainEntity", "instanceOf", "intendedAudience"], instanceAfterChange.data)

        if (valueBefore != null && valueAfter != null && valueBefore instanceof List && valueAfter instanceof List) {
            if (valueAfter as Set != valueBefore as Set)
                return new Tuple(true, valueBefore, valueAfter)
        }
        return new Tuple(false, null, null)
    }

    static Tuple mainTitleChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object titlesBefore = Document._get(["mainEntity", "hasTitle"], instanceBeforeChange.data)
        Object titlesAfter = Document._get(["mainEntity", "hasTitle"], instanceAfterChange.data)

        Map oldMainTitle = null
        Map newMainTitle = null

        if (titlesBefore != null && titlesAfter != null && titlesBefore instanceof List && titlesAfter instanceof List) {

            for (Object oBefore : titlesBefore) {
                Map titleBefore = (Map) oBefore
                if (titleBefore["mainTitle"] && titleBefore["@type"] == "Title")
                    oldMainTitle = titleBefore
            }

            for (Object oAfter : titlesAfter) {
                Map titleAfter = (Map) oAfter
                if (titleAfter["mainTitle"] && titleAfter["@type"] == "Title")
                    newMainTitle = titleAfter
            }

            if (newMainTitle != null && oldMainTitle != null && !newMainTitle.equals(oldMainTitle))
                return new Tuple(true, oldMainTitle, newMainTitle)
        }
        return new Tuple(false, null, null)
    }

    static Tuple keyTitleChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object titlesBefore = Document._get(["mainEntity", "hasTitle"], instanceBeforeChange.data)
        Object titlesAfter = Document._get(["mainEntity", "hasTitle"], instanceAfterChange.data)

        Map oldMainTitle = null
        Map newMainTitle = null

        if (titlesBefore != null && titlesAfter != null && titlesBefore instanceof List && titlesAfter instanceof List) {

            for (Object oBefore : titlesBefore) {
                Map titleBefore = (Map) oBefore
                if (titleBefore["mainTitle"] && titleBefore["@type"] == "KeyTitle")
                    oldMainTitle = titleBefore
            }

            for (Object oAfter : titlesAfter) {
                Map titleAfter = (Map) oAfter
                if (titleAfter["mainTitle"] && titleAfter["@type"] == "KeyTitle")
                    newMainTitle = titleAfter
            }

            if (newMainTitle != null && oldMainTitle != null && !newMainTitle.equals(oldMainTitle))
                return new Tuple(true, oldMainTitle, newMainTitle)
        }
        return new Tuple(false, null, null)
    }

    static Tuple DDCChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object classificationsBefore = Document._get(["mainEntity", "instanceOf", "classification"], instanceBeforeChange.data)
        Object classificationsAfter = Document._get(["mainEntity", "instanceOf", "classification"], instanceAfterChange.data)

        Map oldDDC = null
        Map newDDC = null

        if (classificationsBefore != null && classificationsAfter != null && classificationsBefore instanceof List && classificationsAfter instanceof List) {

            for (Object oBefore : classificationsBefore) {
                Map classificationBefore = (Map) oBefore
                if (classificationBefore["@type"] && classificationBefore["@type"] == "ClassificationDdc")
                    oldDDC = classificationBefore
            }

            for (Object oAfter : classificationsAfter) {
                Map classificationAfter = (Map) oAfter
                if (classificationAfter["@type"] && classificationAfter["@type"] == "ClassificationDdc")
                    newDDC = classificationAfter
            }

            if (newDDC != null && oldDDC != null && !newDDC.equals(oldDDC))
                return new Tuple(true, oldDDC, newDDC)
        }
        return new Tuple(false, null, null)
    }

    static Tuple SABChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object classificationsBefore = Document._get(["mainEntity", "instanceOf", "classification"], instanceBeforeChange.data)
        Object classificationsAfter = Document._get(["mainEntity", "instanceOf", "classification"], instanceAfterChange.data)

        Map oldSAB = null
        Map newSAB = null

        if (classificationsBefore != null && classificationsAfter != null && classificationsBefore instanceof List && classificationsAfter instanceof List) {

            for (Object oBefore : classificationsBefore) {
                Map classificationBefore = (Map) oBefore
                if (classificationBefore["inScheme"] && classificationBefore["inScheme"]["code"] &&
                        classificationBefore["inScheme"]["code"] == "kssb")
                    oldSAB = classificationBefore
            }

            for (Object oAfter : classificationsAfter) {
                Map classificationAfter = (Map) oAfter
                if (classificationAfter["inScheme"] && classificationAfter["inScheme"]["code"] &&
                        classificationAfter["inScheme"]["code"] == "kssb")
                    newSAB = classificationAfter
            }

            if (newSAB != null && oldSAB != null && !newSAB.equals(oldSAB))
                return new Tuple(true, oldSAB, newSAB)
        }
        return new Tuple(false, null, null)
    }

}
