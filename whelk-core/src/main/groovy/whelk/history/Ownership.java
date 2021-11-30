package whelk.history;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

public class Ownership {
    public String m_manualEditor;
    public Instant m_manualEditTime;

    public String m_systematicEditor;
    public String m_systematicEditorComment;
    public Instant m_systematicEditTime;

    public Ownership(DocumentVersion version, Ownership previousOwnership) {

        // An initial version from voyager
        if (version.changedIn.equals("vcopy") )
        {
            m_manualEditor = "Voyager";
            m_manualEditTime = ZonedDateTime.parse(version.doc.getModified()).toInstant();
        }
        // A handmade (or atleast REST-API entered) version
        else if (version.changedIn.equals("xl") && version.changedBy != null && !version.changedBy.endsWith(".groovy")) {
            m_manualEditor = version.changedBy;
            m_manualEditTime = ZonedDateTime.parse(version.doc.getModified()).toInstant();
        }
        // An import or script generated version (systematic edit)
        else {
            if (previousOwnership != null) {
                m_manualEditor = previousOwnership.m_manualEditor;
                m_manualEditTime = previousOwnership.m_manualEditTime;
            }

            setSystemChangeDescription(version.changedBy, version.changedIn);

            Instant modifiedInstant = ZonedDateTime.parse(version.doc.getModified()).toInstant();
            Instant generatedInstant = Instant.EPOCH;
            if (version.doc.getGenerationDate() != null)
                generatedInstant = ZonedDateTime.parse(version.doc.getGenerationDate()).toInstant();
            if (modifiedInstant.isAfter( generatedInstant ))
                m_systematicEditTime = modifiedInstant;
            else
                m_systematicEditTime = generatedInstant;
        }
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (m_manualEditTime != null && m_manualEditor != null) {
            sb.append("[M]" + m_manualEditor + " at: " + m_manualEditTime + " ");
        }
        if (m_systematicEditTime != null && m_systematicEditor != null) {
            sb.append("[S]" + m_systematicEditor + " ");
            if (m_systematicEditorComment != null)
                sb.append("(" + m_systematicEditorComment + ") ");
            sb.append("at: " + m_systematicEditTime);
        }

        return sb.toString();
    }

    public static String getSystemChangeDescription(String changedBy, String changedIn) {
        if (changedBy != null && changedIn != null && changedIn.equals("APIX")) {
            return "APIX (" + changedBy + ")";
        } else if (changedBy != null && changedIn != null && changedIn.equals("batch import")) {
            return "Metadatatratten (" + changedBy + ")";
        }
        return "Scripted (XL administrative)";
    }

    private void setSystemChangeDescription(String changedBy, String changedIn) {
        m_systematicEditor = changedBy;
        m_systematicEditorComment = getSystemChangeDescription(changedBy, changedIn);
    }
}
