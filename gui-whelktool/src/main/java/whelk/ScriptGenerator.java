package whelk;

import java.util.Set;

public class ScriptGenerator
{
    public static PortableScript generateDeleteHoldScript(String sigel, Set<String> controlNumbers)
    {
        String where = "\"id in\n" +
                "(\n" +
                " select lh.id\n" +
                "  from\n" +
                "   lddb lb\n" +
                "  left join\n" +
                "   lddb lh\n" +
                "  on lh.data#>>'{@graph,1,itemOf,@id}' = lb.data#>>'{@graph,1,@id}'\n" +
                " where lb.data#>>'{@graph,0,controlNumber}' in ( 'bibidstring' )\n" +
                " and\n" +
                " lh.data#>>'{@graph,1,heldBy,@id}' = 'https://libris.kb.se/library/Mde'\n" +
                ")\"";

        String scriptText = "" +
                "PrintWriter failedHoldIDs = getReportWriter(\"failed-to-delete-holdIDs\")\n" +
                "PrintWriter scheduledForDeletion = getReportWriter(\"scheduled-for-deletion\")\n" +
                "File bibids = new File(scriptDir, '£INPUT')\n" +
                "\n" +
                "String bibidstring = bibids.readLines().join(\"','\")\n" +
                "\n" +
                "selectBySqlWhere(" + where + ", silent: false, { hold ->\n" +
                "    scheduledForDeletion.println(\"${hold.doc.getURI()}\")\n" +
                "    hold.scheduleDelete(onError: { e ->\n" +
                "        failedHoldIDs.println(\"Failed to delete ${hold.doc.shortId} due to: $e\")\n" +
                "    })\n" +
                "})\n";

        return new PortableScript(scriptText, controlNumbers);
    }

}
