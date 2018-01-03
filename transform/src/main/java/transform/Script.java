package transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Script
{
    private List<String> m_operations = new ArrayList<>();

    // Temporary state, held only during a single MOVE resolution
    private List<String> head = new ArrayList<>();
    private List<String> tail = new ArrayList<>();
    private List<String> fromDiff;
    private List<String> toDiff;

    public void resolveMove(String fromPath, String toPath)
    {
        //System.err.println("Attempting to resolve " + fromPath + " INTO " + toPath);

        List<String> from = Arrays.asList(fromPath.split(","));
        List<String> to = Arrays.asList(toPath.split(","));

        // make:
        // from = head + fromDiff + tail
        // to = head + toDiff + tail
        head = new ArrayList<>();
        tail = new ArrayList<>();
        fromDiff = new ArrayList<>();
        toDiff = new ArrayList<>();
        int i = 0;
        while ( from.get(i).equals(to.get(i)) )
            head.add( from.get(i++) );
        i = 0;
        while ( from.get( from.size()-i-1 ).equals(to.get( to.size()-i-1 )) )
        {
            tail.add(0, from.get(from.size() - i - 1));
            ++i;
        }
        fromDiff = from.subList( head.size(), from.size()-tail.size() );
        toDiff = to.subList( head.size(), to.size()-tail.size() );

        //System.err.println("head: " + head + " tail: " + tail + "\n\tfromDiff: " + fromDiff + "\n\ttoDiff: " + toDiff);

        List<String> operations = generatePivotPointMoves();
        if (!operations.isEmpty())
        {
            m_operations.add("# Resulting from observed grammar diff:\n# " + fromPath + " -> " + toPath);
            m_operations.addAll(operations);
            m_operations.add(""); // empty line
        }

        for (String op : m_operations)
            System.out.println(op);
    }

    /**
     * Lists ("_list") forms pivots points of sorts in the transformation. In the diffing part of the path,
     * each list must be matched up to its transformed equivalence. If the number of "_list"s diff, the change
     * can't be resolved with any certainty (Which list should be collapsed?).
     */
    private List<String> generatePivotPointMoves()
    {
        List<String> resultingOperations = new ArrayList<>();

        boolean done;

        // Generate a move sequence for each _list pivot point
        do {
            done = true;
            int fromIndex = 0, toIndex = 0;
            while (fromIndex < fromDiff.size() && toIndex < toDiff.size())
            {
                if (fromDiff.get(fromIndex).equals("_list") && toDiff.get(toIndex).equals("_list"))
                {
                    List<String> sourceList = new ArrayList<>();
                    sourceList.addAll(head);
                    sourceList.addAll(fromDiff.subList(0, fromIndex));

                    List<String> targetList = new ArrayList<>();
                    targetList.addAll(head);
                    targetList.addAll(toDiff.subList(0, toIndex));

                    // Issue a command to make this move
                    resultingOperations.addAll(generateMoveSequence(sourceList, targetList, 0, 0));

                    // Keep looking
                    head.addAll(toDiff.subList(0, toIndex+1));
                    toDiff = toDiff.subList(toIndex+1, toDiff.size());
                    fromDiff = fromDiff.subList(fromIndex+1, fromDiff.size());
                    done = false;
                    break;
                }

                if (!fromDiff.get(fromIndex).equals("_list"))
                    ++fromIndex;

                if (!toDiff.get(toIndex).equals("_list"))
                    ++toIndex;
            }
        } while (!done);

        //System.err.println("Remaining: " + fromDiff + " / " + toDiff);

        // Generate a move sequence for the remainder [last pivotpoint] -> end of toPath
        List<String> sourceList = new ArrayList<>();
        sourceList.addAll(head);
        sourceList.addAll(fromDiff);
        List<String> targetList = new ArrayList<>();
        targetList.addAll(head);
        targetList.addAll(toDiff);
        resultingOperations.addAll(generateMoveSequence(sourceList, targetList, 0, 0));

        return resultingOperations;
    }

    private List<String> generateMoveSequence(List<String> sourcePath, List<String> targetPath, int startIndex, int indentation)
    {
        List<String> resultingOperations = new ArrayList<>();

        for (int i = startIndex; i < Integer.min(sourcePath.size(), targetPath.size()); ++i)
        {
            if (sourcePath.get(i).equals("_list") && targetPath.get(i).equals("_list"))
            {
                List<String> newSourcePath = new ArrayList<>();
                newSourcePath.addAll(sourcePath.subList(0, i));
                newSourcePath.add("it"+indentation);
                newSourcePath.addAll(sourcePath.subList(i+1, sourcePath.size()));

                List<String> newTargetPath = new ArrayList<>();
                newTargetPath.addAll(targetPath.subList(0, i));
                newTargetPath.add("it"+indentation);
                newTargetPath.addAll(targetPath.subList(i+1, targetPath.size()));

                String tabs = "";
                for (int j = 0; j < indentation; ++j)
                    tabs += "   ";
                String tabsP1 = "";
                for (int j = 0; j < indentation+1; ++j)
                    tabsP1 += "   ";

                resultingOperations.add(tabs + "FOREACH it" + indentation + " : " + String.join(",", sourcePath.subList(0, i+1)));
                resultingOperations.add(tabs + "{");
                List<String> nestedOps = generateMoveSequence(newSourcePath, newTargetPath, startIndex+1, indentation+1);
                resultingOperations.addAll(nestedOps);
                if (nestedOps.isEmpty())
                    resultingOperations.add(tabsP1 + "MOVE " + String.join(",",newSourcePath) + " -> " + String.join(",",newTargetPath));
                resultingOperations.add(tabs + "}");
                break;
            }
        }

        return resultingOperations;
    }
}
