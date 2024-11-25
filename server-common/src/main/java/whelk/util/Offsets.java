package whelk.util;

// TODO class is duplicated in three places
public class Offsets {
    public Integer prev;
    public Integer next;
    public Integer first;
    public Integer last;

    public Offsets(int total, int limit, int offset) throws IllegalArgumentException {
        if (limit < 0) {
            throw new IllegalArgumentException("\"limit\" can't be negative.");
        }

        if (offset < 0) {
            throw new IllegalArgumentException("\"offset\" can't be negative.");
        }

        if (limit == 0) {
            return;
        }

        if (offset != 0) {
            this.first = 0;
        }

        this.prev = offset - limit;
        if (this.prev < 0) {
            this.prev = null;
        }

        this.next = offset + limit;
        if (this.next >= total) {
            this.next = null;
        } else if (offset == 0) {
            this.next = limit;
        }

        if (total % limit == 0) {
            this.last = total - limit;
        } else {
            this.last = total - (total % limit);
        }
    }

    public boolean hasNext() {
        return this.next != null;
    }

    public boolean hasPrev() {
        return this.prev != null;
    }

    public boolean hasLast() {
        return this.last != null;
    }

    public boolean hasFirst() {
        return this.first != null;
    }
}
