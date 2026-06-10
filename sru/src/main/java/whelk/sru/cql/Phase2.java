package whelk.sru.cql;

public class Phase2 {
    public static String flatten(Object ast) {
        switch (ast) {

            case Phase1.AstSearchClause searchClause -> {
                return flatten(searchClause);
            }
            case Phase1.AstScopedClause scopedClause -> {
                return flatten(scopedClause);
            }
            case Phase1.AstBooleanGroup bg -> {
                return flatten(bg);
            }
            case String s -> {
                return scrubXlQlString(s);
            }
            default -> {
                return null;
            }
        }
    }

    /**
     All 'strings' passed along as part of an XLQL query must be quoted and stripped of internal '\' and '"' chars (so there can be no escape from the quote).
     */
    private static String scrubXlQlString(String str) {
        str = str.replace('\\', ' ');
        str = str.replace('"', ' ');
        return "\"" + str.toLowerCase().trim() + "\"";
    }

    private static String flatten(Phase1.AstScopedClause scopedClause) {
        // scopedClause, booleanGroup, searchClause. Either all three or just the searchClause

        if (scopedClause.scopedClause() == null) // just the searchClause
            return flatten(scopedClause.searchClause());
        else { // scoped + boolean
            return "(" + flatten(scopedClause.scopedClause()) + flatten(scopedClause.booleanGroup()) + flatten(scopedClause.searchClause()) + ")";
        }
    }

    private static String flatten(Phase1.AstBooleanGroup booleanGroup) {
        // boolean [modifierList]

        // Ignore relation modifiers. What does "or/rel.combine=sum" even mean? Compared to just "or" ?

        String op = booleanGroup.op();
        if (op.equalsIgnoreCase("AND") || op.equalsIgnoreCase("OR") || op.equalsIgnoreCase("NOT"))
            op = op.toUpperCase();
        // "CQL supports a fourth boolean operator, proximity. This is a special kind of ``and'' which requires its operands to occur close to each other."
        if (op.equalsIgnoreCase("PROX"))
            op = "AND";
        return " " + op + " ";
    }

    private static String flatten(Phase1.AstSearchClause searchClause) {
        // index, term, relation

        String searchTerm = scrubXlQlString(searchClause.term());

        if (searchClause.relation() == null) { // no relation also implies no index. It's just searchTerm alone.
            return " \"" + searchTerm + "\n";
        }

        Phase1.AstRelation relation = searchClause.relation();

        if (relation.comparitor().equals("any")) {
            // "any" means we're searching for any of the words in the searchTerm. So 'any "a b"' means (a or b)
            String[] parts = searchTerm.split("\\s+");
            if (parts.length > 1)
                searchTerm = "(" + String.join("\" OR \"", parts) + ")";
        } else if (relation.comparitor().equals("all")) {
            // "all" means we're searching for all of the words in the searchTerm. So 'all "a b"' means (a and b)
            String[] parts = searchTerm.split("\\s+");
            if (parts.length > 1)
                searchTerm = "(" + String.join("\" AND \"", parts) + ")";
        }

        String index = searchClause.index();

        // A crowd-pleaser, lets interpret these codes (missing the .cpl-prefix) in a neighbourly way.
        if (index.equals("anywhere") || index.equals("allIndexes") || index.equals("anyIndexes") || index.equals("serverChoice"))
            index = "cql.anywhere";

        boolean indexNeedsQuoting = ! ( index.startsWith("\"") && index.endsWith("\"") );
        if (indexNeedsQuoting)
            index = "\"" + index + "\"";

        return index + "=" + searchTerm;
    }
}
