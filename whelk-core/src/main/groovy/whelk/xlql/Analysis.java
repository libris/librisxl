package whelk.xlql;

import java.util.ArrayList;
import java.util.List;

public class Analysis {

    static Object flattenCodes(Object astNode) {

        // If a CodeEquals is found, recreate all children with the code
        if (astNode instanceof Ast.CodeEquals) {
            String code = ((Ast.CodeEquals) astNode).code();

            Object operand = ((Ast.CodeEquals) astNode).operand();
            if (operand instanceof Ast.And) {
                List<Object> newOperands = new ArrayList<>();
                for (Object o : ((Ast.And) operand).operands()) {
                    newOperands.add(wrapAllChildrenInCode(code, o));
                }
                return new Ast.And(newOperands);
            }
            if (operand instanceof Ast.Or) {
                List<Object> newOperands = new ArrayList<>();
                for (Object o : ((Ast.Or) operand).operands()) {
                    newOperands.add(wrapAllChildrenInCode(code, o));
                }
                return new Ast.Or(newOperands);
            }
            if (operand instanceof Ast.Not) {
                return new Ast.Not(wrapAllChildrenInCode(code, ((Ast.Not) operand).operand()));
            }
            if (operand instanceof Ast.Like) {
                return new Ast.Like(wrapAllChildrenInCode(code, ((Ast.Like) operand).operand()));
            }

        }

        // Until a CodeEquals is found, recreate as is
        if (astNode instanceof Ast.And) {
            List<Object> newOperands = new ArrayList<>();
            for (Object o : ((Ast.And) astNode).operands()) {
                newOperands.add(flattenCodes(o));
            }
            return new Ast.And(newOperands);
        }
        if (astNode instanceof Ast.Or) {
            List<Object> newOperands = new ArrayList<>();
            for (Object o : ((Ast.Or) astNode).operands()) {
                newOperands.add(flattenCodes(o));
            }
            return new Ast.Or(newOperands);
        }
        if (astNode instanceof Ast.Not) {
            return new Ast.Not(flattenCodes(((Ast.Not) astNode).operand()));
        }
        if (astNode instanceof Ast.Like) {
            return new Ast.Like(flattenCodes(((Ast.Like) astNode).operand()));
        }

        return astNode; // leafs, like String, no more checking needed
    }

    static Object wrapAllChildrenInCode(String code, Object astNode) {
        if (astNode instanceof String) {
            return new Ast.CodeEquals(code, astNode);
        } else if (astNode instanceof Ast.And) {
            List<Object> replacementOperands = new ArrayList<>();
            for (Object child : ((Ast.And) astNode).operands()) {
                replacementOperands.add(wrapAllChildrenInCode(code, child));
            }
            return new Ast.And(replacementOperands);
        } else if (astNode instanceof Ast.Or) {
            List<Object> replacementOperands = new ArrayList<>();
            for (Object child : ((Ast.Or) astNode).operands()) {
                replacementOperands.add(wrapAllChildrenInCode(code, child));
            }
            return new Ast.Or(replacementOperands);
        } else if (astNode instanceof Ast.Not) {
            return new Ast.Not(wrapAllChildrenInCode(code, ((Ast.Not) astNode).operand()));
        } else if (astNode instanceof Ast.Like) {
            return new Ast.Like(wrapAllChildrenInCode(code, ((Ast.Like) astNode).operand()));
        }
        throw new RuntimeException("XLQL Error when flattening: " + astNode); // Should not be reachable. This is a bug.
    }

    static void checkSemantics(Object astNode) throws BadQueryException {
        checkNoCodeWithinCode(astNode, false);
    }

    /**
     * Language constructs like for example code:("something" and code:"whatever") pass parsing, but make no
     * sense and should be considered bad queries.
     */
    static void checkNoCodeWithinCode(Object astNode, boolean inCodeGroup) throws BadQueryException {
        if (astNode instanceof Ast.And) {
            for (Object child : ((Ast.And) astNode).operands()) {
                checkNoCodeWithinCode(child, inCodeGroup);
            }
        } else if (astNode instanceof Ast.Or) {
            for (Object child : ((Ast.Or) astNode).operands()) {
                checkNoCodeWithinCode(child, inCodeGroup);
            }
        } else if (astNode instanceof Ast.Not) {
            checkNoCodeWithinCode(((Ast.Not) astNode).operand(), inCodeGroup);
        } else if (astNode instanceof Ast.Like) {
            checkNoCodeWithinCode(((Ast.Like) astNode).operand(), inCodeGroup);
        } else if (astNode instanceof Ast.CodeLesserGreaterThan) {
            if (inCodeGroup)
                throw new BadQueryException("Codes within code groups are not allowed.");
        } else if (astNode instanceof Ast.CodeEquals) {
            if (inCodeGroup)
                throw new BadQueryException("Codes within code groups are not allowed.");
            else
                checkNoCodeWithinCode(((Ast.CodeEquals) astNode).operand(), true);
        }
    }
}
