package whelk;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

public class PortableScript implements Serializable
{
    final String scriptText;
    final Set<String> ids;

    public PortableScript(String scriptText, Set<String> ids)
    {
        this.scriptText = scriptText;
        this.ids = Collections.unmodifiableSet(ids);
    }

    public void execute() throws IOException
    {
        Path scriptWorkingDir = Files.createTempDirectory("xl_script");
        Path scriptFilePath = scriptWorkingDir.resolve("script.groovy");
        Path inputFilePath = scriptWorkingDir.resolve("input");

        Files.write(inputFilePath, ids);
        String flattenedScriptText = scriptText.replace("£INPUT", inputFilePath.toString());
        Files.write(scriptFilePath, flattenedScriptText.getBytes());

        String[] args =
                {
                        scriptFilePath.toString()
                };

        whelk.datatool.WhelkTool.main(args);
    }
}