package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, ArrayList<Field>> pair;
    private ArrayList<Field> single;
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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if(gbfield != Aggregator.NO_GROUPING){
            this.pair = new HashMap<Field, ArrayList<Field>>();
        }
        else{
            this.single = new ArrayList<Field>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(gbfield != Aggregator.NO_GROUPING){
            Field theGroup = tup.getField(gbfield);
            Field theValue = tup.getField(afield);

            ArrayList<Field> vals = pair.get(theGroup);
            if(vals != null)
                vals.add(theValue);
            else{
                vals = new ArrayList<Field>();
                vals.add(theValue);
            }
            pair.put(theGroup,vals);
        }
        else{
            single.add(tup.getField(afield));
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
        if(this.gbfield == Aggregator.NO_GROUPING){
            int size = single.size();

            TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});
            Tuple tup = new Tuple(td);
            tup.setField(0, new IntField(size));

            ArrayList<Tuple> tuparray = new ArrayList<Tuple>();
            tuparray.add(tup);
            return new TupleIterator(td, tuparray);
        }

        else{
            ArrayList<Tuple> tupArr = new ArrayList<Tuple>();
            TupleDesc td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});

            Set<Field> keys = pair.keySet();
            Iterator<Field> keysIterator = keys.iterator();
            while(keysIterator.hasNext()){
                Field f = keysIterator.next();
                ArrayList valuesList = pair.get(f);
                int count = valuesList.size();

                Tuple t = new Tuple(td);
                t.setField(0, f);
                t.setField(1, new IntField(count));
                tupArr.add(t);
            }

            return new TupleIterator(td, tupArr);
        }
        //throw new UnsupportedOperationException("please implement me for lab2");
    }

}
