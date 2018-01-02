package transform;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Script
{
    private List<String> m_operations = new ArrayList<>();

    public void resolveMove(String fromPath, String toPath)
    {
        //System.err.println("Attempting to resolve " + fromPath + " INTO " + toPath);

        List<String> from = Arrays.asList(fromPath.split(","));
        List<String> to = Arrays.asList(toPath.split(","));

        // make:
        // from = head + fromDiff + tail
        // to = head + toDiff + tail
        List<String> head = new ArrayList<>();
        List<String> tail = new ArrayList<>();
        List<String> fromDiff;
        List<String> toDiff;
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

        System.err.println("head: " + head + " tail: " + tail + "\n\tfromDiff: " + fromDiff + "\n\ttoDiff: " + toDiff);

        List<String> operations = generatePivotPointMoves(head, fromDiff, toDiff);
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
    private List<String> generatePivotPointMoves(List<String> head, List<String> fromDiff, List<String> toDiff)
    {
        List<String> resultingOperations = new ArrayList<>();

        int fromIndex = 0, toIndex = 0;
        while (fromIndex < fromDiff.size() && toIndex < toDiff.size())
        {
            if ( fromDiff.get(fromIndex).equals("_list") && toDiff.get(toIndex).equals("_list") )
            {
                List<String> sourceList = new ArrayList<>();
                sourceList.addAll(head);
                sourceList.addAll(fromDiff.subList(0, fromIndex));
                //String source = String.join(",", sourceList);

                List<String> targetList = new ArrayList<>();
                targetList.addAll(head);
                targetList.addAll(toDiff.subList(0, toIndex));
                //String target = String.join(",", targetList);

                // Issue a command to make this move
                //System.err.println("MOVE " + source + " -> " + target);
                resultingOperations.addAll( generateMoveSequence(sourceList, targetList) );

                // Keep looking
                ++fromIndex;
                ++toIndex;
                continue;
            }

            if ( ! fromDiff.get(fromIndex).equals("_list"))
                ++fromIndex;

            if ( ! toDiff.get(toIndex).equals("_list"))
                ++toIndex;
        }

        return resultingOperations;
    }

    private List<String> generateMoveSequence(List<String> sourcePath, List<String> targetPath)
    {
        List<String> resultingOperations = new ArrayList<>();

        for (int i = 0; i < Integer.min(sourcePath.size(), targetPath.size()); ++i)
        {
            if (sourcePath.get(i).equals("_list") && targetPath.get(i).equals("_list"))
            {
                resultingOperations.add("FOREACH it : " + String.join(",", sourcePath.subList(0, i)));
                resultingOperations.add("{");
                resultingOperations.add("MOVE " +
                        String.join(",", sourcePath.subList(0, i)) + ",it," + String.join(",", sourcePath.subList(i+1, sourcePath.size())) + " -> " +
                        String.join(",", targetPath.subList(0, i)) + ",it," + String.join(",", targetPath.subList(i+1, targetPath.size())) );
                resultingOperations.add("}");
            }
        }

        return resultingOperations;
    }
}
