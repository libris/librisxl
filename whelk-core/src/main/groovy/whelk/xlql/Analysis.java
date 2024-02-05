package whelk.xlql;

import whelk.exception.InvalidQueryException;

public class Analysis {
    static void checkSemantics(Ast.Node ast) throws InvalidQueryException {
        checkNoCodeWithinCode(ast, false);
    }

    /**
     * Language constructs like for example code:("something" and code:"whatever") pass parsing, but make no
     * sense and should be considered bad queries.
     */
    private static void checkNoCodeWithinCode(Ast.Node astNode, boolean inCodeGroup) throws InvalidQueryException {
        switch (astNode) {
            case Ast.And and -> {
                for (Ast.Node child : and.operands()) {
                    checkNoCodeWithinCode(child, inCodeGroup);
                }
            }
            case Ast.Or or -> {
                for (Ast.Node child : or.operands()) {
                    checkNoCodeWithinCode(child, inCodeGroup);
                }
            }
            case Ast.Not not -> {
                checkNoCodeWithinCode(not.operand(), inCodeGroup);
            }
            case Ast.CodeEquals ce -> {
                if (inCodeGroup) {
                    throw new InvalidQueryException("Codes within code groups are not allowed.");
                }
                checkNoCodeWithinCode(ce.operand(), true);
            }
            case Ast.CodeEqualsLeaf ignored -> {
                if (inCodeGroup) {
                    throw new InvalidQueryException("Codes within code groups are not allowed.");
                }
            }
            case Ast.CodeLesserGreaterThan ignored -> {
                if (inCodeGroup) {
                    throw new InvalidQueryException("Codes within code groups are not allowed.");
                }
            }
            case Ast.Leaf ignored -> {
                // Nothing to do here
            }
        }
    }
}
