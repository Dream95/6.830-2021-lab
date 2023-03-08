package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;

    private final TupleDesc tupleDesc;

    private final Map<Field, Integer> map = new HashMap<>();

    private final Map<Field, Integer> countMap = new HashMap();

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        List<Type> typeAr = new ArrayList<>();
        if (gbfieldtype != null) {
            typeAr.add(gbfieldtype);
        }
        typeAr.add(Type.INT_TYPE);
        this.tupleDesc = new TupleDesc(typeAr.toArray(new Type[0]));
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        boolean isGroup = this.gbfieldtype != null;
        Field key = isGroup ? tup.getField(gbfield) : new IntField(0);
        int value = map.getOrDefault(key, what == Op.MIN ? Integer.MAX_VALUE : 0);
        int tupValue = ((IntField) tup.getField(afield)).getValue();
        switch (what) {
            case MIN:
                value = Math.min(value, tupValue);
                break;
            case MAX:
                value = Math.max(value, tupValue);
                break;
            case AVG:
                value += tupValue;
                break;
            case SUM:
                value += tupValue;
                break;
        }
        map.put(key, value);
        countMap.put(key, countMap.getOrDefault(key, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new InAggregatorOpIterator();
    }

    private class InAggregatorOpIterator implements OpIterator {

        private Iterator<Field> iterator;

        public InAggregatorOpIterator() {

        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.iterator = map.keySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return this.iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tuple = new Tuple(tupleDesc);
            Field key = iterator.next();

            int value;
            switch (what) {
                case COUNT:
                    value = countMap.get(key);
                    break;
                case AVG:
                    value = map.get(key) / countMap.get(key);
                    break;
                default:
                    value = map.get(key);
            }
            if (gbfieldtype == null) {
                tuple.setField(0, new IntField(value));
            } else {
                tuple.setField(0, key);
                tuple.setField(1, new IntField(value));
            }
            return tuple;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tupleDesc;
        }

        @Override
        public void close() {
            this.iterator = null;
        }
    }
}
