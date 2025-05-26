package whelk.housekeeping;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import whelk.Whelk;
import whelk.datatool.Script;
import whelk.datatool.WhelkTool;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;

public class ScriptRunner extends HouseKeeper {
    private String status = "OK";
    private final String scriptName;
    private final String schedule;
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final Whelk premadeWhelk;

    public ScriptRunner(Whelk premadeWhelk, String scriptName, String schedule) {
        this.scriptName = scriptName;
        this.schedule = schedule;
        this.premadeWhelk = premadeWhelk;
    }

    public String getName() {
        return "Scriptrunner: " + scriptName;
    }

    public String getStatusDescription() {
        return status;
    }

    public String getCronSchedule() {
        return schedule;
    }

    public void trigger() {
        try {
            InputStream scriptStream = ScriptRunner.class.getClassLoader().getResourceAsStream(scriptName);
            assert scriptStream != null;
            String scriptText = IOUtils.toString(new InputStreamReader(scriptStream));
            String scriptUri = "/sys/housekeeping/" + scriptName;
            Script script = new Script(scriptText, scriptUri);

            Path reportPath = Files.createTempDirectory("housekeeping_script_execution").resolve("report");

            WhelkTool tool = new WhelkTool(premadeWhelk, script, reportPath.toFile(), WhelkTool.getDEFAULT_STATS_NUM_IDS());
            tool.setAllowLoud(true);
            tool.setNoThreads(false);
            tool.setNumThreads(Runtime.getRuntime().availableProcessors());
            tool.run();

            Path errorLogPath = reportPath.resolve("ERRORS.txt");
            if (Files.size(errorLogPath) > 0) {
                String firstError = "[could not read]";
                try (BufferedReader errorLogReader = Files.newBufferedReader(errorLogPath)) {
                    firstError = errorLogReader.readLine();
                }
                String errorMessage = "Execution of script (" + scriptName +  ") resulted in non-zero size ERRORS.txt, first line of which is: "
                        + firstError + "\nSee: " + errorLogPath;
                logger.error(errorMessage);
                status = errorMessage;
            } else {
                status = "OK";
            }

        } catch (Exception e) {
            logger.error("Failed script (" + scriptName + ") execution attempt: ", e);
            throw new RuntimeException(e);
        }

    }
}