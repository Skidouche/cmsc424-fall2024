package edu.umd.cs424.database.table;

import java.util.Iterator;

import edu.umd.cs424.database.BaseTransaction;
import edu.umd.cs424.database.common.BacktrackingIterator;
import edu.umd.cs424.database.DatabaseException;

/**
 * A RecordIterator wraps an Iterator<RecordId> to form an Iterator<Record>.
 * For example,
 *
 *   Iterator<RecordId> ridIterator = getRecordIdIterator();
 *   RecordIterator recordIterator = new RecordIterator(t, ridIterator);
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 *   recordIterator.next(); // equivalent to t.getRecord(ridIterator.next())
 */
public class RecordIterator implements BacktrackingIterator<Record> {
    private Iterator<RecordId> ridIter;
    private Table table;
    private BaseTransaction transaction;

    public RecordIterator(BaseTransaction transaction, Table table, Iterator<RecordId> ridIter) {
        this.ridIter = ridIter;
        this.table = table;
        this.transaction = transaction;
    }

    public RecordIterator(Table table, Iterator<RecordId> ridIter) {
        this(null, table, ridIter);
    }

    public boolean hasNext() {
        return ridIter.hasNext();
    }

    public Record next() {
        try {
            return table.getRecord(transaction, ridIter.next());
        } catch (DatabaseException e) {
            throw new IllegalStateException(e);
        }
    }

    public void mark() {
        if (ridIter instanceof BacktrackingIterator) {
            ((BacktrackingIterator) ridIter).mark();
        } else {
            throw new UnsupportedOperationException("Cannot mark using underlying iterator");
        }
    }

    public void reset() {
        if (ridIter instanceof BacktrackingIterator) {
            ((BacktrackingIterator) ridIter).reset();
        } else {
            throw new UnsupportedOperationException("Cannot reset using underlying iterator");
        }
    }
}

