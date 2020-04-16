package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    HashMap<Field, Integer> cnt;
    java.util.concurrent.ConcurrentHashMap<Field, Integer> vals;
    private static final Field NO_KEY = new IntField(0);

    /**
     * Aggregate constructor
     * 
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or
     *                    null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.vals = new ConcurrentHashMap<Field, Integer>();
        this.cnt = new HashMap<Field, Integer>();
        if (Aggregator.NO_GROUPING == this.gbfield) {
            cnt.put(NO_KEY, 0);
            vals.put(NO_KEY, 0);
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = NO_KEY;
        if (gbfield != Aggregator.NO_GROUPING) {
            key = tup.getField(gbfield);
        }

        int val = ((IntField) (tup.getField(this.afield))).getValue();

        // merges tuples iff field matches.
        if (this.gbfield == Aggregator.NO_GROUPING
                || tup.getTupleDesc().getFieldType(this.gbfield).equals(this.gbfieldtype)) {
            if (!(vals.containsKey(key))) {
                vals.put(key, val);
                cnt.put(key, 1);
            } else {
                int aux = 0;
                if (what == Op.MIN) {
                    aux = Math.min(vals.get(key), val);
                } else if (what == Op.MAX) {
                    aux = Math.max(vals.get(key), val);
                } else if (what == Op.AVG || what == Op.SUM) {
                    aux = vals.get(key) + val;
                } else if (what == Op.COUNT) {
                    aux = vals.get(key) + 1;
                }
                vals.put(key, aux);
                cnt.put(key, cnt.get(key) + 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal) if
     *         using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in the
     *         constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc td;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        if (this.gbfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });

            Tuple t = new Tuple(td);

            int value = this.vals.get(NO_KEY);
            if (this.what == Aggregator.Op.AVG) {
                value /= this.cnt.get(NO_KEY);
            } else if (this.what == Aggregator.Op.COUNT) {
                value = this.cnt.get(NO_KEY);
            }

            t.setField(0, new IntField(value));
            tuples.add(t);
        } else {
            td = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
            Enumeration<Field> keys = this.vals.keys();

            while (keys.hasMoreElements()) {
                Tuple t = new Tuple(td);
                Field key = keys.nextElement();
                int value = this.vals.get(key);
                if (this.what == Aggregator.Op.AVG) {
                    value /= this.cnt.get(key);
                } else if (this.what == Aggregator.Op.COUNT) {
                    value = this.cnt.get(key);
                }

                t.setField(0, key);
                t.setField(1, new IntField(value));
                tuples.add(t);
            }
        }

        return new TupleIterator(td, tuples);
        // throw new
        // UnsupportedOperationException("please implement me for lab2");
    }
}
