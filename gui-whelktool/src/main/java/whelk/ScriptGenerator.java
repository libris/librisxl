package whelk;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

public class ScriptGenerator
{
    public static PortableScript generateDeleteHoldScript(String sigel, Set<String> controlNumbers) throws IOException
    {
        String scriptText = IOUtils.toString(new InputStreamReader(
                ScriptGenerator.class.getClassLoader().getResourceAsStream("templates/delete-hold.groovy") ));
        scriptText = scriptText.replace("£SIGEL", sigel);

        return new PortableScript(scriptText, controlNumbers, "Radering av " + sigel + "-bestånd");
    }

    public static PortableScript generateCreateHoldScript(String sigel, Set<String> controlNumbers) throws IOException
    {
        String scriptText = IOUtils.toString(new InputStreamReader(
                ScriptGenerator.class.getClassLoader().getResourceAsStream("templates/create-hold.groovy") ));
        scriptText = scriptText.replace("£SIGEL", sigel);

        return new PortableScript(scriptText, controlNumbers, "Skapande av " + sigel + "-bestånd");
    }

    public static PortableScript generateDeleteBibScript(Set<String> controlNumbers) throws IOException
    {
        String scriptText = IOUtils.toString(new InputStreamReader(
                ScriptGenerator.class.getClassLoader().getResourceAsStream("templates/delete-bib.groovy") ));

        return new PortableScript(scriptText, controlNumbers, "Radering av " + controlNumbers.size() + " bibbliografiska IDn");
    }

    public static PortableScript generateChangeSigelScript(String fromSigel, String toSigel) throws IOException
    {
        String scriptText = IOUtils.toString(new InputStreamReader(
                ScriptGenerator.class.getClassLoader().getResourceAsStream("templates/change-sigel.groovy") ));
        scriptText = scriptText.replace("£FROMSIGEL", fromSigel);
        scriptText = scriptText.replace("£TOSIGEL", toSigel);

        return new PortableScript(scriptText, null, "Byte av sigel " + fromSigel + " till " + toSigel);
    }
}
