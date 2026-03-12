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
            default -> {
                return null;
            }
            /* // irrelevant, can't be root nodes
            case Phase1.AstBooleanGroup bg -> {
                return flatten(bg);
            }
            case Phase1.AstModifier mod -> {
                return null;
            }
            case Phase1.AstRelation rel -> {
                return flatten(rel);
            }*/
        }
    }

    private static String flatten(Phase1.AstScopedClause scopedClause) {
        // scopedClause, booleanGroup, searchClause. Either all three or just the searchClause

        if (scopedClause.scopedClause() == null) // just the searchClause
            return flatten(scopedClause.searchClause());
        else { // scoped + boolean
            return flatten(scopedClause.scopedClause()) + flatten(scopedClause.booleanGroup()) + flatten(scopedClause.searchClause());
        }
    }

    private static String flatten(Phase1.AstBooleanGroup booleanGroup) {
        // boolean [modifierList]

        // Ignore relation modifiers. What does "or/rel.combine=sum" even mean? Compared to just "or" ?
        return " " + booleanGroup.op() + " ";
    }

    private static String flatten(Phase1.AstSearchClause searchClause) {
        // index, term, relation
        if (searchClause.relation() == null) { // no relation also implies no index. It's just searchTerm alone.
            return " " + searchClause.term() + " ";
        }

        Phase1.AstRelation relation = searchClause.relation();
        String searchTerm = searchClause.term();
        if (relation.comparitor().equals("any")) {
            // "any" means we're searching for any of the words in the searchTerm. So 'any "a b"' means (a or b)
            String[] parts = searchTerm.split("\\s+");
            searchTerm = "(" + String.join(" OR ", parts) + ")";
        } else if (relation.comparitor().equals("all")) {
            // "all" means we're searching for all of the words in the searchTerm. So 'all "a b"' means (a and b)
            String[] parts = searchTerm.split("\\s+");
            searchTerm = "(" + String.join(" AND ", parts) + ")";
        }

        return  " " + searchClause.index() + ":" + searchClause.term() + " ";
    }
}
