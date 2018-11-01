/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package se.kb.libris.export.dewey;

/**
 *
 * @author marma
 */
    public class Mapping {
        int rowid;
        String dewey, sab;
        Relation relation;
        Usage usage;

        public Mapping(int _rowid, String _sab, String _dewey, Relation _relation, Usage _usage) {
            rowid = _rowid;
            dewey = _dewey;
            sab = _sab;
            relation = _relation;
            usage = _usage;
        }

        public String getDewey() {
            return dewey;
        }

        public String getSAB() {
            return sab;
        }

        public Usage getUsage() {
            return usage;
        }

        public Relation getRelation() {
            return relation;
        }

        @Override
        public String toString() {
            return "<Mapping: " + rowid + ", '" + sab + "', '" + dewey + "', " + relation + ", " + usage + ">";
        }

        public int getDeweyScore() {
            int score = 0;

            if (relation == Relation.EXACT) score += 100;
            if (usage == Usage.MAIN) score += 9;
            if (usage == Usage.LEFT) score += 5;
            if (usage == Usage.RIGHT) score += 2;
                
            return score;
        }

        public int getSabScore() {
            int score = 0;

            if (relation == Relation.EXACT) score += 100;
            if (usage == Usage.MAIN) score += 9;
            if (usage == Usage.LEFT) score += 2;
            if (usage == Usage.RIGHT) score += 5;

            return score;
        }
    }
