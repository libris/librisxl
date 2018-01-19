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
     * each list must be matched up to its transformed equivalence. If the number of "_list"s diff, the rightmost lists
     * are selected to be collapsed.
     */
    private List<String> generatePivotPointMoves(List<String> from, List<String> to)
    {
        final String indentation = "    ";
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
        int targetPathLists = 0;
        for (String node: to)
        {
            if (node.equals("_list"))
            {
                // Only replace a _list node with a itX node if there's a corresponding list in the from path.
                // otherwise, just pick the first element (0).
                if (targetPathLists < listsAtFromDiffIndex.size())
                    targetList.add("it" + (level++));
                else
                    targetList.add("0");
                ++targetPathLists;
            }
            else
                targetList.add(node);
        }

        String tabs = "";
        for (int i = 0; i < listsAtFromDiffIndex.size(); ++i)
        {
            resultingOperations.add(tabs + "FOREACH it" + i + " : " + String.join(",", sourceList.subList(0, listsAtFromDiffIndex.get(i))));
            resultingOperations.add(tabs + "{");
            tabs += indentation;
        }

        resultingOperations.add(tabs + "MOVE " + String.join(",",sourceList) +
                "\n" + tabs + "  -> " + String.join(",",targetList));

        for (int i = 0; i < listsAtFromDiffIndex.size(); ++i)
        {
            tabs = "";
            for (int j = 1; j < (listsAtFromDiffIndex.size()-i); ++j)
                tabs += indentation;
            resultingOperations.add(tabs + "}");
        }

        return resultingOperations;
    }
}
