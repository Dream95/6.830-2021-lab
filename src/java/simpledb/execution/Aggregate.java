package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private final OpIterator child;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private final TupleDesc tupleDesc;

    private Aggregator aggregator;

    private OpIterator aggOpIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        List<Type> typeAr = new ArrayList<>();
        List<String> typeName = new ArrayList<>();
        if (gfield != -1) {
            typeAr.add(child.getTupleDesc().getFieldType(gfield));
            typeName.add(child.getTupleDesc().getFieldName(gfield));
        }
        typeAr.add(child.getTupleDesc().getFieldType(afield));
        typeName.add(child.getTupleDesc().getFieldName(afield));
        this.tupleDesc = new TupleDesc(typeAr.toArray(new Type[0]), typeName.toArray(new String[0]));
        this.aop = aop;


    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (gfield != -1) {
            return this.tupleDesc.getFieldName(0);
        }
        return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        if (gfield == -1) {
            return this.tupleDesc.getFieldName(0);
        } else {
            return this.tupleDesc.getFieldName(1);
        }
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        if (this.child.hasNext()) {
            Tuple next = this.child.next();
            Type gType = null;

            if (this.gfield != -1) {
                gType = next.getField(gfield).getType();
            }

            if (this.aggregator == null) {
                Field field = next.getField(afield);
                if (Objects.equals(field.getType(), Type.INT_TYPE)) {
                    this.aggregator = new IntegerAggregator(gfield, gType, afield, aop);
                } else {
                    this.aggregator = new StringAggregator(gfield, gType, afield, aop);
                }
            }
            this.aggOpIterator = this.aggregator.iterator();
            this.aggregator.mergeTupleIntoGroup(next);
            while (this.child.hasNext()) {
                this.aggregator.mergeTupleIntoGroup(this.child.next());
            }
            this.aggOpIterator.open();
        }



    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.aggOpIterator.hasNext()){
            return this.aggOpIterator.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.close();
        this.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.aggregator = null;
        this.aggOpIterator = null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
