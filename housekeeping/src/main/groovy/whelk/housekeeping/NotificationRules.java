package whelk.housekeeping;

import whelk.Document;
import whelk.util.Unicode;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

public class NotificationRules {

    /**
     * The outcome of comparing one property between two versions of a record.
     */
    public record ChangeResult(boolean changed, Object before, Object after) {
        static ChangeResult unchanged() {
            return new ChangeResult(false, null, null);
        }
    }

    private static boolean lifeSpanChanged(Object lifeSpanBefore, Object lifeSpanAfter) {
        if ((lifeSpanBefore == null && lifeSpanAfter != null) || (lifeSpanBefore != null && lifeSpanAfter == null))
            return false;
        if (!(lifeSpanBefore instanceof String before) || !(lifeSpanAfter instanceof String after))
            return false;

        int dashAtIndexBefore = before.indexOf('-');
        if (dashAtIndexBefore == -1)
            return false;
        String birthBefore = before.substring(0, dashAtIndexBefore).trim();
        String deathBefore = before.substring(dashAtIndexBefore + 1).trim();

        int dashAtIndexAfter = after.indexOf('-');
        if (dashAtIndexAfter == -1)
            return false;
        String birthAfter = after.substring(0, dashAtIndexAfter).trim();
        String deathAfter = after.substring(dashAtIndexAfter + 1).trim();

        if (!birthBefore.isEmpty() && !birthBefore.equals(birthAfter))
            return true;

        if (!deathBefore.isEmpty() && !deathBefore.equals(deathAfter))
            return true;

        return false;
    }

    private static boolean personChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map<?, ?> before) || !(agentAfter instanceof Map<?, ?> after))
            return false;

        if (Objects.equals(before.get("@type"), "Person") && Objects.equals(after.get("@type"), "Person")) {
            return stringChanged((String) before.get("familyName"), (String) after.get("familyName")) ||
                    stringChanged((String) before.get("givenName"), (String) after.get("givenName")) ||
                    stringChanged((String) before.get("name"), (String) after.get("name")) ||
                    lifeSpanChanged(before.get("lifeSpan"), after.get("lifeSpan"));
        }
        return false;
    }

    private static boolean organizationChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map<?, ?> before) || !(agentAfter instanceof Map<?, ?> after))
            return false;

        if (Objects.equals(before.get("@type"), "Organization") && Objects.equals(after.get("@type"), "Organization")) {
            if (!Objects.equals(before.get("name"), after.get("name")))
                return true;
            if (before.get("isPartOf") instanceof Map<?, ?> partOfBefore &&
                    after.get("isPartOf") instanceof Map<?, ?> partOfAfter) {
                if (!Objects.equals(partOfBefore.get("name"), partOfAfter.get("name")) ||
                        !Objects.equals(before.get("marc:subordinateUnit"), after.get("marc:subordinateUnit")))
                    return true;
            }
        }
        return false;
    }

    private static boolean meetingChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map<?, ?> before) || !(agentAfter instanceof Map<?, ?> after))
            return false;

        if (Objects.equals(before.get("@type"), "Meeting") && Objects.equals(after.get("@type"), "Meeting")) {
            return !Objects.equals(before.get("place"), after.get("place")) ||
                    !Objects.equals(before.get("date"), after.get("date")) ||
                    !Objects.equals(before.get("name"), after.get("name"));
        }
        return false;
    }

    private static boolean jurisdictionChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map<?, ?> before) || !(agentAfter instanceof Map<?, ?> after))
            return false;

        if (Objects.equals(before.get("@type"), "Jurisdiction") && Objects.equals(after.get("@type"), "Jurisdiction")) {
            return !Objects.equals(before, after); // For now
        }
        return false;
    }

    private static boolean familyChanged(Object agentBefore, Object agentAfter) {
        if (!(agentBefore instanceof Map<?, ?> before) || !(agentAfter instanceof Map<?, ?> after))
            return false;

        if (Objects.equals(before.get("@type"), "Family") && Objects.equals(after.get("@type"), "Family")) {
            return !Objects.equals(before, after); // For now
        }
        return false;
    }

    static ChangeResult agentRecordChanged(Document recordBeforeChange, Document recordAfterChange) {
        Object agentBefore = Document._get(List.of("mainEntity"), recordBeforeChange.data);
        Object agentAfter = Document._get(List.of("mainEntity"), recordAfterChange.data);

        if (personChanged(agentBefore, agentAfter) ||
                meetingChanged(agentBefore, agentAfter) ||
                organizationChanged(agentBefore, agentAfter) ||
                familyChanged(agentBefore, agentAfter) ||
                jurisdictionChanged(agentBefore, agentAfter)) {
            return new ChangeResult(true, agentBefore, agentAfter);
        }
        return ChangeResult.unchanged();
    }

    static ChangeResult primaryContributionChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object contributionsAfter = Document._get(List.of("mainEntity", "instanceOf", "contribution"), instanceAfterChange.data);
        Object contributionsBefore = Document._get(List.of("mainEntity", "instanceOf", "contribution"), instanceBeforeChange.data);

        if (contributionsBefore instanceof List<?> beforeList && contributionsAfter instanceof List<?> afterList) {
            for (Object oBefore : beforeList) {
                for (Object oAfter : afterList) {
                    if (!(oBefore instanceof Map<?, ?> contrBefore) || !(oAfter instanceof Map<?, ?> contrAfter))
                        continue;
                    if (!Objects.equals(contrBefore.get("@type"), "PrimaryContribution") ||
                            !Objects.equals(contrAfter.get("@type"), "PrimaryContribution"))
                        continue;

                    Object agentBefore = contrBefore.get("agent");
                    Object agentAfter = contrAfter.get("agent");
                    if (agentBefore == null || agentAfter == null)
                        continue;

                    if (personChanged(agentBefore, agentAfter) ||
                            meetingChanged(agentBefore, agentAfter) ||
                            organizationChanged(agentBefore, agentAfter)) {
                        return new ChangeResult(true, agentBefore, agentAfter);
                    }
                }
            }
        }
        return ChangeResult.unchanged();
    }

    static ChangeResult subjectChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object subjectsAfter = Document._get(List.of("mainEntity", "instanceOf", "subject"), instanceAfterChange.data);
        Object subjectsBefore = Document._get(List.of("mainEntity", "instanceOf", "subject"), instanceBeforeChange.data);

        List<Object> removedSubjects = new ArrayList<>();
        List<Object> addedSubjects = new ArrayList<>();

        if (subjectsBefore instanceof List<?> beforeList && subjectsAfter instanceof List<?> afterList) {

            // Find removed
            for (Object subjectBefore : beforeList) {
                boolean subjectExistsAfter = false;
                for (Object subjectAfter : afterList) {
                    if (subjectUnchanged(subjectBefore, subjectAfter)) {
                        subjectExistsAfter = true;
                    }
                }
                if (!subjectExistsAfter) {
                    removedSubjects.add(subjectBefore);
                }
            }

            // Find added
            for (Object subjectAfter : afterList) {
                boolean subjectExistsBefore = false;
                for (Object subjectBefore : beforeList) {
                    if (subjectUnchanged(subjectBefore, subjectAfter)) {
                        subjectExistsBefore = true;
                    }
                }
                if (!subjectExistsBefore) {
                    addedSubjects.add(subjectAfter);
                }
            }

            if (!addedSubjects.isEmpty() || !removedSubjects.isEmpty()) {
                return new ChangeResult(true, removedSubjects, addedSubjects);
            }
        }
        return ChangeResult.unchanged();
    }

    private static boolean subjectUnchanged(Object subjectBefore, Object subjectAfter) {
        return !personChanged(subjectBefore, subjectAfter) &&
                !meetingChanged(subjectBefore, subjectAfter) &&
                !jurisdictionChanged(subjectBefore, subjectAfter) &&
                !familyChanged(subjectBefore, subjectAfter) &&
                !organizationChanged(subjectBefore, subjectAfter);
    }

    static ChangeResult serialRelationChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        if (!Objects.equals(Document._get(List.of("mainEntity", "issuanceType"), instanceBeforeChange.data), "Serial"))
            return ChangeResult.unchanged();
        if (!Objects.equals(Document._get(List.of("mainEntity", "issuanceType"), instanceAfterChange.data), "Serial"))
            return ChangeResult.unchanged();

        Object continuedByBefore = Document._get(List.of("mainEntity", "continuedBy"), instanceBeforeChange.data);
        Object continuedByAfter = Document._get(List.of("mainEntity", "continuedBy"), instanceAfterChange.data);
        if (continuedByBefore instanceof List<?> beforeList && continuedByAfter instanceof List<?> afterList) {
            if (!titles(afterList).equals(titles(beforeList)))
                return new ChangeResult(true, continuedByBefore, continuedByAfter);
        }

        Object continuesBefore = Document._get(List.of("mainEntity", "continues"), instanceBeforeChange.data);
        Object continuesAfter = Document._get(List.of("mainEntity", "continues"), instanceAfterChange.data);
        if (continuesBefore instanceof List<?> beforeList && continuesAfter instanceof List<?> afterList) {
            if (!titles(afterList).equals(titles(beforeList)))
                return new ChangeResult(true, continuesBefore, continuesAfter);
        }

        return ChangeResult.unchanged();
    }

    private static Set<Object> titles(List<?> relations) {
        Set<Object> titles = new LinkedHashSet<>();
        for (Object relation : relations) {
            titles.add(relation instanceof Map<?, ?> m ? m.get("hasTitle") : null);
        }
        return titles;
    }

    static ChangeResult serialTerminationChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        if (!Objects.equals(Document._get(List.of("mainEntity", "issuanceType"), instanceBeforeChange.data), "Serial"))
            return ChangeResult.unchanged();
        if (!Objects.equals(Document._get(List.of("mainEntity", "issuanceType"), instanceAfterChange.data), "Serial"))
            return ChangeResult.unchanged();

        Object publicationsBefore = Document._get(List.of("mainEntity", "publication"), instanceBeforeChange.data);
        Object publicationsAfter = Document._get(List.of("mainEntity", "publication"), instanceAfterChange.data);

        if (publicationsBefore instanceof List<?> beforeList && publicationsAfter instanceof List<?> afterList &&
                beforeList.size() == afterList.size()) {
            for (int i = 0; i < beforeList.size(); ++i) {
                Object endYearBefore = beforeList.get(i) instanceof Map<?, ?> m ? m.get("endYear") : null;
                Object endYearAfter = afterList.get(i) instanceof Map<?, ?> m ? m.get("endYear") : null;
                if (!Objects.equals(endYearBefore, endYearAfter)) {
                    Object before = isTruthy(endYearBefore) ? beforeList.get(i) : null;
                    Object after = afterList.get(i);
                    return new ChangeResult(true, before, after);
                }
            }
        }

        return ChangeResult.unchanged();
    }

    static ChangeResult intendedAudienceChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        Object valueBefore = Document._get(List.of("mainEntity", "instanceOf", "intendedAudience"), instanceBeforeChange.data);
        Object valueAfter = Document._get(List.of("mainEntity", "instanceOf", "intendedAudience"), instanceAfterChange.data);

        if (valueBefore instanceof List<?> beforeList && valueAfter instanceof List<?> afterList) {
            if (!new LinkedHashSet<>(afterList).equals(new LinkedHashSet<>(beforeList)))
                return new ChangeResult(true, valueBefore, valueAfter);
        }
        return ChangeResult.unchanged();
    }

    static ChangeResult mainTitleChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        return titleChanged(instanceBeforeChange, instanceAfterChange, "Title");
    }

    static ChangeResult keyTitleChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        return titleChanged(instanceBeforeChange, instanceAfterChange, "KeyTitle");
    }

    private static ChangeResult titleChanged(Document instanceBeforeChange, Document instanceAfterChange, String titleType) {
        Object titlesBefore = Document._get(List.of("mainEntity", "hasTitle"), instanceBeforeChange.data);
        Object titlesAfter = Document._get(List.of("mainEntity", "hasTitle"), instanceAfterChange.data);

        Map<?, ?> oldMainTitle = null;
        Map<?, ?> newMainTitle = null;

        if (titlesBefore instanceof List<?> beforeList && titlesAfter instanceof List<?> afterList) {

            for (Object oBefore : beforeList) {
                Map<?, ?> titleBefore = (Map<?, ?>) oBefore;
                if (isTruthy(titleBefore.get("mainTitle")) && Objects.equals(titleBefore.get("@type"), titleType))
                    oldMainTitle = titleBefore;
            }

            for (Object oAfter : afterList) {
                Map<?, ?> titleAfter = (Map<?, ?>) oAfter;
                if (isTruthy(titleAfter.get("mainTitle")) && Objects.equals(titleAfter.get("@type"), titleType))
                    newMainTitle = titleAfter;
            }

            if (newMainTitle != null && oldMainTitle != null &&
                    stringChanged((String) newMainTitle.get("mainTitle"), (String) oldMainTitle.get("mainTitle")))
                return new ChangeResult(true, oldMainTitle, newMainTitle);
        }
        return ChangeResult.unchanged();
    }

    static ChangeResult DDCChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        return classificationChanged(instanceBeforeChange, instanceAfterChange,
                classification -> Objects.equals(classification.get("@type"), "ClassificationDdc"));
    }

    static ChangeResult SABChanged(Document instanceBeforeChange, Document instanceAfterChange) {
        return classificationChanged(instanceBeforeChange, instanceAfterChange, classification ->
                classification.get("inScheme") instanceof Map<?, ?> inScheme &&
                        Objects.equals(inScheme.get("code"), "kssb"));
    }

    private static ChangeResult classificationChanged(Document instanceBeforeChange, Document instanceAfterChange,
                                                      Predicate<Map<?, ?>> isRelevant) {
        List<Object> classificationsBefore = classifications(instanceBeforeChange);
        List<Object> classificationsAfter = classifications(instanceAfterChange);

        Set<Object> oldMatches = new LinkedHashSet<>();
        Set<Object> newMatches = new LinkedHashSet<>();

        if (!classificationsBefore.isEmpty() && !classificationsAfter.isEmpty()) {

            for (Object oBefore : classificationsBefore) {
                Map<?, ?> classificationBefore = (Map<?, ?>) oBefore;
                if (isRelevant.test(classificationBefore))
                    oldMatches.add(classificationBefore);
            }

            for (Object oAfter : classificationsAfter) {
                Map<?, ?> classificationAfter = (Map<?, ?>) oAfter;
                if (isRelevant.test(classificationAfter))
                    newMatches.add(classificationAfter);
            }

            if (!newMatches.equals(oldMatches))
                return new ChangeResult(true, oldMatches, newMatches);
        }
        return ChangeResult.unchanged();
    }

    private static List<Object> classifications(Document instance) {
        List<Object> classifications = new ArrayList<>();
        if (Document._get(List.of("mainEntity", "instanceOf", "classification"), instance.data) instanceof List<?> workClassifications)
            classifications.addAll(workClassifications);
        if (Document._get(List.of("mainEntity", "classification"), instance.data) instanceof List<?> instanceClassifications)
            classifications.addAll(instanceClassifications);
        return classifications;
    }

    private static boolean stringChanged(String before, String after) {
        if (before == null && after != null)
            return true;
        if (before != null && after == null)
            return true;
        if (before == null)
            return false;

        String a = normalize(before);
        String b = normalize(after);
        if (a.length() > Unicode.MAX_LEVENSHTEIN_LENGTH || b.length() > Unicode.MAX_LEVENSHTEIN_LENGTH) {
            return !a.equals(b);
        } else {
            return Unicode.damerauLevenshteinDistance(a, b, Unicode.MAX_LEVENSHTEIN_LENGTH) > 1;
        }
    }

    private static String normalize(String s) {
        return Unicode.removeDiacritics(s.toLowerCase()).replaceAll("[^\\p{Alnum}]", "");
    }

    /**
     * Groovy truth, for the values encountered here: null and the empty String are falsy.
     */
    private static boolean isTruthy(Object o) {
        if (o == null)
            return false;
        if (o instanceof String s)
            return !s.isEmpty();
        return true;
    }
}
