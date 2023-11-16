package whelk.housekeeping

import whelk.Document

class NotificationRules {
    static Tuple primaryContributionChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object contributionsAfter = Document._get(["mainEntity", "instanceOf", "contribution"], instanceAfterChange.data)
        Object contributionsBefore = Document._get(["mainEntity", "instanceOf", "contribution"], instanceBeforeChange.data)
        if (contributionsBefore != null && contributionsAfter != null && contributionsBefore instanceof List && contributionsAfter instanceof List) {
            for (Object contrBefore : contributionsBefore) {
                for (Object contrAfter : contributionsAfter) {
                    if (contrBefore["@type"].equals("PrimaryContribution") && contrAfter["@type"].equals("PrimaryContribution")) {
                        if (contrBefore["agent"] != null && contrAfter["agent"] != null) {
                            if (
                                    contrBefore["agent"]["familyName"] != contrAfter["agent"]["familyName"] ||
                                            contrBefore["agent"]["givenName"] != contrAfter["agent"]["givenName"] ||
                                            contrBefore["agent"]["lifeSpan"] != contrAfter["agent"]["lifeSpan"]
                            )
                                return new Tuple(true, contrBefore["agent"], contrAfter["agent"])
                        }
                    }
                }
            }
        }
        return new Tuple(false, null, null)
    }

    static Tuple primaryPublicationChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object publicationsAfter = Document._get(["mainEntity", "publication"], instanceAfterChange.data)
        Object publicationsBefore = Document._get(["mainEntity", "publication"], instanceBeforeChange.data)
        if (publicationsBefore != null && publicationsAfter != null && publicationsBefore instanceof List && publicationsAfter instanceof List) {
            for (Object pubBefore : publicationsBefore) {
                for (Object pubAfter : publicationsAfter) {
                    if (pubBefore["@type"].equals("PrimaryPublication") && pubAfter["@type"].equals("PrimaryPublication")) {
                        if (!pubBefore["year"].equals(pubAfter["year"])) {
                            return new Tuple(true, pubBefore["year"], pubAfter["year"])
                        }
                    }
                }
            }
        }
        return new Tuple(false, null, null)
    }

    static Tuple SerialRelationChanged(Document instanceBeforeChange, Document instanceAfterChange) {

        if (!Document._get(["issuanceType"], instanceBeforeChange.data).equals("Serial"))
            return new Tuple(false, null, null)
        if (!Document._get(["issuanceType"], instanceAfterChange.data).equals("Serial"))
            return new Tuple(false, null, null)

        Object precededBefore = Document._get(["precededBy"], instanceBeforeChange.data)
        Object precededAfter = Document._get(["precededBy"], instanceAfterChange.data)
        if (precededBefore != null && precededAfter != null && precededBefore instanceof List && precededAfter instanceof List) {
            if (precededAfter as Set != precededBefore as Set)
                return new Tuple(true, precededBefore, precededAfter)
        }

        Object succeededBefore = Document._get(["succeededBy"], instanceBeforeChange.data)
        Object succeededAfter = Document._get(["succeededBy"], instanceAfterChange.data)
        if (succeededBefore != null && succeededAfter != null && succeededBefore instanceof List && succeededAfter instanceof List) {
            if (succeededAfter as Set != succeededBefore as Set)
                return new Tuple(true, succeededBefore, succeededAfter)
        }

        return new Tuple(false, null, null)
    }

    static Tuple SerialTerminationChanged(Document instanceBeforeChange, Document instanceAfterChange) {

        if (!Document._get(["issuanceType"], instanceBeforeChange.data).equals("Serial"))
            return new Tuple(false, null, null)
        if (!Document._get(["issuanceType"], instanceAfterChange.data).equals("Serial"))
            return new Tuple(false, null, null)

        Object publicationsBefore = Document._get(["publication"], instanceBeforeChange.data)
        Object publicationsAfter = Document._get(["publication"], instanceAfterChange.data)

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
                if (titleBefore["mainTitle"])
                    oldMainTitle = titleBefore
            }

            for (Object oAfter : titlesAfter) {
                Map titleAfter = (Map) oAfter
                if (titleAfter["mainTitle"])
                    newMainTitle = titleAfter
            }

            if (newMainTitle != null && oldMainTitle != null && !newMainTitle.equals(oldMainTitle))
                return new Tuple(true, oldMainTitle, newMainTitle)
        }
        return new Tuple(false, null, null)
    }

    static Tuple primaryTitleChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object titlesBefore = Document._get(["mainEntity", "hasTitle"], instanceBeforeChange.data)
        Object titlesAfter = Document._get(["mainEntity", "hasTitle"], instanceAfterChange.data)

        Map oldPrimaryTitle = null
        Map newPrimaryTitle = null

        if (titlesBefore != null && titlesAfter != null && titlesBefore instanceof List && titlesAfter instanceof List) {

            for (Object oBefore : titlesBefore) {
                Map titleBefore = (Map) oBefore
                if (titleBefore["@type"] && titleBefore["@type"] == "Title")
                    oldPrimaryTitle = titleBefore
            }

            for (Object oAfter : titlesAfter) {
                Map titleAfter = (Map) oAfter
                if (titleAfter["@type"] && titleAfter["@type"] == "Title")
                    newPrimaryTitle = titleAfter
            }

            if (newPrimaryTitle != null && oldPrimaryTitle != null && !newPrimaryTitle.equals(oldPrimaryTitle))
                return new Tuple(true, oldPrimaryTitle, newPrimaryTitle)
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
