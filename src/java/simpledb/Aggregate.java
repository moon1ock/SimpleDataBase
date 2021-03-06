package simpledb;

import java.util.*;
import java.lang.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private int afield;
    private OpIterator aggregator;
    private int gfield;
    private OpIterator child;
    private Aggregator.Op aop;

    /**
     * Constructor.
     *
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     */
    public int groupField() {
        if (gfield != -1)
            return gfield;
        else
            return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name of
     *         the groupby field in the <b>OUTPUT</b> tuples If not, return null;
     */
    public String groupFieldName() {
        if (gfield != Aggregator.NO_GROUPING)
            return child.getTupleDesc().getFieldName(gfield);
        else
            return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b> tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException, TransactionAbortedException {
        super.open();
        child.open();

        Type aggregatorType = child.getTupleDesc().getFieldType(afield);
        Type groupType = null;
        Aggregator aggregate;
        if (groupField() != Aggregator.NO_GROUPING)
            groupType = child.getTupleDesc().getFieldType(gfield);

        if (aggregatorType == Type.STRING_TYPE) {
            aggregate = new StringAggregator(gfield, groupType, afield, aop);
            while (child.hasNext())
                aggregate.mergeTupleIntoGroup(child.next());
        } else {// If the Type is INT
            aggregate = new IntegerAggregator(gfield, groupType, afield, aop);
            while (child.hasNext())
                aggregate.mergeTupleIntoGroup(child.next());
        }
        child.close();
        this.aggregator = aggregate.iterator();
        this.aggregator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first field is
     * the field by which we are grouping, and the second field is the result of
     * computing the aggregate, If there is no group by field, then the result tuple
     * should contain one field representing the result of the aggregate. Should
     * return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (this.aggregator.hasNext())
            return this.aggregator.next();
        else {
            return null;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggregator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field, this
     * will have one field - the aggregate column. If there is a group by field, the
     * first field will be the group by field, and the second will be the aggregate
     * value column.
     *
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are given
     * in the constructor, and child_td is the TupleDesc of the child iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc td = this.child.getTupleDesc();
        Type type = td.getFieldType(this.afield);
        String name = td.getFieldName(this.afield);

        if (this.groupField() != Aggregator.NO_GROUPING) {
            Type groupType = td.getFieldType(this.gfield);
            String groupName = td.getFieldName(this.gfield);
            return new TupleDesc(new Type[] { type, groupType }, new String[] { name, groupName });// returns a pair
        } else {// if it is NO Grouping
            return new TupleDesc(new Type[] { type }, new String[] { name });

        }

    }

    public void close() {
        aggregator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { aggregator };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}