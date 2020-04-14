package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;
import java.util.Enumeration;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ConcurrentHashMap<Field, Integer> values;
    private ConcurrentHashMap<Field, String> extremum; // Extreme values (MAX/MIN)

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
            throw new IllegalArgumentException("The operation is not supported");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.values = new ConcurrentHashMap<Field, Integer>();
        this.extremum = new ConcurrentHashMap<Field, String>();


    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field fieldKey = new IntField(0);
        String stringVal;
        if(gbfield != Aggregator.NO_GROUPING)
            fieldKey = tup.getField(gbfield);


        if (tup.getTupleDesc().getFieldType(gbfield).equals(gbfieldtype) || gbfield == Aggregator.NO_GROUPING){
            if(what == Op.COUNT){
                if(values.containsKey(fieldKey)){//if the key is on the map
                    values.put(fieldKey, values.get(fieldKey) + 1); //increment by one
                }
                else{//if not
                    values.put(fieldKey, 1);//this is the first elem with such key
                }
            }
            else if(what == Op.MAX || what == Op.MIN){ //if the operation is not COUNT
                stringVal = ((StringField)  (tup.getField(afield))).getValue();
                if(!extremum.containsKey(fieldKey)) {//if the key is not present
                    extremum.put(fieldKey, stringVal);//puts a new element with key fieldKey
                }
                else if(what == Op.MAX){//if key is in the map and operation is MAX
                    if (extremum.get(fieldKey).compareTo(stringVal) > 0) { // if the value the key holds is greater than stringVal
                        extremum.put(fieldKey, extremum.get(fieldKey));
                    }
                    else {
                        extremum.put(fieldKey, stringVal);
                    }
                } else if (what == Op.MIN) {//if key is in the map and operation is MIN
                    if(extremum.get(fieldKey).compareTo(stringVal) < 0){// if the value the key holds is less than stringVal
                        extremum.put(fieldKey, extremum.get(fieldKey));
                    }
                    else{
                        extremum.put(fieldKey, stringVal);
                    }
                }
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
        TupleDesc td;
        Tuple tuple;
        Field fieldKey;
        Enumeration<Field> fieldKeys;
        ArrayList<Tuple> lot = new ArrayList<Tuple>();// list of tuples
        if(what == Op.MAX || what == Op.MIN) {// operator is either MAX or MIN
            td = new TupleDesc(new Type[]{gbfieldtype, Type.STRING_TYPE});
            fieldKeys = extremum.keys();
            while (fieldKeys.hasMoreElements()) {
                fieldKey = fieldKeys.nextElement();
                tuple = new Tuple(td);
                tuple.setField(0, fieldKey);
                tuple.setField(1, new StringField(extremum.get(fieldKey), 1048));
                lot.add(tuple);
            }
        }
        else { // if COUNT
            td = new TupleDesc (new Type[]{gbfieldtype, Type.INT_TYPE});
            fieldKeys = values.keys();
            while (fieldKeys.hasMoreElements()) {
                fieldKey = fieldKeys.nextElement();
                tuple = new Tuple(td);
                tuple.setField(0, fieldKey);
                tuple.setField(1, new IntField(values.get(fieldKey)));
                lot.add(tuple);
            }
        }
        return new TupleIterator(td, lot);
    }

}