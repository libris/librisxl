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

}
