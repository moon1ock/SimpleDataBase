package simpledb;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;

import jdk.nashorn.internal.runtime.arrays.ArrayLikeIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByFeild;
    private Type groupByFieldType;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> vals;
    private ConcurrentHashMap<Field, String> minMaxVals;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what!=Op.COUNT && what!=Op.MAX && what != Op.MIN)
            throw new IllegalArgumentException("what is not MIN/MAX/COUNT");
        this.groupByFeild= gbfield;
        this.groupByFieldType = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.vals = new ConcurrentHashMap<Field, Integer>();
        this.minMaxVals = new ConcurrentHashMap<Field, String>();


    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key = new IntField(0);

        if(this.groupByFeild != Aggregator.NO_GROUPING)
            key = tup.getField(this.groupByFeild);


        if (this.groupByFeild == Aggregator.NO_GROUPING || tup.getTupleDesc().getFieldType(this.groupByFeild).equals(this.groupByFieldType)){
            String tupleString;
            switch (this.what){
                case COUNT:
                    this.vals.put(key, this.vals.containsKey(key) ? this.vals.get(key)+1 : 1);
                    break;
                case MIN:
                    tupleString = ((StringField)  (tup.getField(this.afield))).getValue();
                    if(!this.minMaxVals.containsKey(key))
                        this.minMaxVals.put(key, tupleString);
                    else
                        this.minMaxVals.put(key, this.minMaxVals.get(key).compareTo(tupleString) < 0 ? this.minMaxVals.get(key) : tupleString);
                    break;
                case MAX:
                    tupleString = ((StringField)  (tup.getField(this.afield))).getValue();
                    if(!this.minMaxVals.containsKey(key))
                        this.minMaxVals.put(key, tupleString);
                    else
                        this.minMaxVals.put(key, this.minMaxVals.get(key).compareTo(tupleString) > 0 ? this.minMaxVals.get(key) : tupleString);
                    break;
                default:
                    throw new IllegalStateException(what.toString());
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc tupleDesc;
        ArrayList<Tuple> listOfTuples = new ArrayList<Tuple>();
        if(this.what == Op.COUNT){
            tupleDesc = new TupleDesc (new TupleDesc(new Type[]{this.groupByFieldType, Type.INT_TYPE}));
            Enumeration<Field> keys = this.vals.keys();
            while (keys.hasMoreElements()) {
                Tuple tuple = new Tuple(tupleDesc);
                Field key = keys.nextElement();
                tuple.setField(0, key);
                tuple.setField(1, new IntField(this.vals.get(key)));
                listOfTuples.add(tuple);
            } }else {
                tupleDesc = new TupleDesc(new Type[]{this.groupByFieldType, Type.STRING_TYPE});
                Enumeration<Field> keys = this.minMaxVals.keys();
                while (keys.hasMoreElements()) {
                    Tuple tuple = new Tuple(tupleDesc);
                    Field key = keys.nextElement();
                    tuple.setField(0, key);
                    tuple.setField(1, new StringField(this.minMaxVals.get(key), 1048));
                    listOfTuples.add(tuple);
            }
        }
        return new TupleIterator(tupleDesc, listOfTuples);
    }

}