package whelk.history;

import java.time.Instant;
import java.time.ZonedDateTime;

public class Ownership {
    public String m_lastManualEditor;
    public Instant m_lastManualEditTime;

    public String m_lastEditor;
    public Instant m_lastEditTime;

    public Ownership(DocumentVersion version, Ownership previousOwnership) {

        // An initial version from voyager
        if (version.changedIn.equals("vcopy") )
        {
            m_lastEditor = m_lastManualEditor = "Voyager";
            m_lastEditTime = m_lastManualEditTime = ZonedDateTime.parse(version.doc.getModified()).toInstant();
        }
        // A handmade (or atleast REST-API entered) version
        else if (version.changedIn.equals("xl") && version.changedBy != null && !version.changedBy.endsWith(".groovy")) {
            m_lastEditor = m_lastManualEditor = version.changedBy;
            m_lastEditTime = m_lastManualEditTime = ZonedDateTime.parse(version.doc.getModified()).toInstant();
        }
        // An import or script generated version
        else {
            if (previousOwnership == null) {
                m_lastManualEditor = getDescription(version.changedBy, version.changedIn);
                m_lastManualEditTime = ZonedDateTime.parse(version.doc.getModified()).toInstant();
            } else {
                m_lastManualEditor = previousOwnership.m_lastManualEditor;
                m_lastManualEditTime = previousOwnership.m_lastManualEditTime;
            }

            m_lastEditor = getDescription(version.changedBy, version.changedIn);

            Instant modifiedInstant = ZonedDateTime.parse(version.doc.getModified()).toInstant();
            Instant generatedInstant = Instant.EPOCH;
            if (version.doc.getGenerationDate() != null)
                generatedInstant = ZonedDateTime.parse(version.doc.getGenerationDate()).toInstant();
            if (modifiedInstant.isAfter( generatedInstant ))
                m_lastEditTime = modifiedInstant;
            else
                m_lastEditTime = generatedInstant;
        }
    }

    private String getDescription(String changedBy, String changedIn) {
        if (changedBy == null)
            changedBy = "N/A";
        if (changedBy.endsWith(".groovy")) {
            return "S (scripted)";
        } else if (changedIn.equals("APIX")) {
            return changedBy + " (apix)";
        } else if (changedIn.equals("batch import")) {
            return changedBy + " (metadatatratten)";
        }
        return changedBy;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (m_lastEditTime.equals(m_lastManualEditTime)) {
            sb.append(m_lastEditor + " at: " + m_lastEditTime);
        }
        else if (m_lastEditTime.isAfter(m_lastManualEditTime)) {
            sb.append(m_lastManualEditor + " and since by " + m_lastEditor + " at: " + m_lastEditTime);
        } else {
            sb.append(m_lastManualEditor + " at: " + m_lastManualEditTime);
        }

        return sb.toString();
    }
}
