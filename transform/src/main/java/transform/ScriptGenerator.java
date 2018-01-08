package transform;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ScriptGenerator
{
    public List<String> m_warnings = new ArrayList<>();
    private List<String> m_operations = new ArrayList<>();

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("# Generated: " + ZonedDateTime.now( ZoneOffset.UTC ) + " (UTC)\n#\n");

        sb.append("# WARNINGS:\n#\n");
        for (String s : m_warnings)
        {
            sb.append(s);
            sb.append("\n");
        }
        if (m_warnings.isEmpty())
            sb.append("# I got 99 problems, but your changes ain't one.\n");

        sb.append("\n# SCRIPT:\n\nMODE FRAMED\n\n");
        for (String s : m_operations)
        {
            sb.append(s);
            sb.append("\n");
        }

        return sb.toString();
    }

    public void resolveMove(String fromPath, String toPath)
    {
        List<String> from = Arrays.asList(fromPath.split(","));
        List<String> to = Arrays.asList(toPath.split(","));

        if (Collections.frequency(from, "_list") != Collections.frequency(to, "_list"))
        {
            m_warnings.add("# I dare not generate code for this diff, because the paths differ in number of lists.\n" +
                    "# Please resolve the diff manually. (Severity: HIGH)" +
                    "#    " + fromPath + "\n# -> " + toPath);
            return;
        }

        List<String> operations = generatePivotPointMoves(from, to);
        if (!operations.isEmpty())
        {
            m_operations.add("# Resulting from observed grammar diff:\n#    " + fromPath + "\n# -> " + toPath);
            m_operations.addAll(operations);
            m_operations.add(""); // empty line
        }
    }

    /**
     * Lists ("_list") forms pivots points of sorts in the transformation. In the diffing part of the path,
     * each list must be matched up to its transformed equivalence. If the number of "_list"s diff, the change
     * can't be resolved with any certainty (Which list should be collapsed?).
     */
    private List<String> generatePivotPointMoves(List<String> from, List<String> to)
    {
        List<String> resultingOperations = new ArrayList<>();

        List<String> sourceList = new ArrayList<>();
        List<String> targetList = new ArrayList<>();

        int level = 0;
        List<Integer> listsAtFromDiffIndex = new ArrayList<>();
        for (int i = 0; i < from.size(); ++i)
        {
            String node = from.get(i);
            if (node.equals("_list"))
            {
                sourceList.add("it" + (level++));
                listsAtFromDiffIndex.add(i);
            }
            else
                sourceList.add(node);
        }

        level = 0;
        for (String node: to)
        {
            if (node.equals("_list"))
                targetList.add("it"+(level++));
            else
                targetList.add(node);
        }

        for (int i = 0; i < listsAtFromDiffIndex.size(); ++i)
        {
            resultingOperations.add("FOREACH it" + i + " : " + String.join(",", sourceList.subList(0, listsAtFromDiffIndex.get(i))));
            resultingOperations.add("{");
        }

        resultingOperations.add("MOVE " + String.join(",",sourceList) + "\n" + "  -> " + String.join(",",targetList));

        for (int i = 0; i < listsAtFromDiffIndex.size(); ++i)
            resultingOperations.add("}");

        return resultingOperations;
    }
}
