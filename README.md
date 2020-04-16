# SimpleDB

# TODO: write readable docs

### created by Andrii Lunin

## Query walkthrough
The following code implements a simple join query between two tables, each consisting of three columns of integers. (The file some_data_file1.dat and some_data_file2.dat are binary representation of the pages from this file). This code is equivalent to the SQL statement:
`SELECT *
FROM some_data_file1, some_data_file2
WHERE some_data_file1.field1 = some_data_file2.field1 AND some_data_file1.id > 1
For more extensive examples of query operations, you may find it helpful to browse the unit tests for joins, filters, and aggregates.
package simpledb; import java.io.*;
public class jointest {
public static void main(String[] argv) {
// construct a 3-column table schema
Type types[] = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE }; String names[] = new String[]{ "field0", "field1", "field2" };
TupleDesc td = new TupleDesc(types, names); 5
 // create the tables, associate them with the data files
// and tell the catalog about the schema the tables.
HeapFile table1 = new HeapFile(new File("some_data_file1.dat"), td); Database.getCatalog().addTable(table1, "t1");
HeapFile table2 = new HeapFile(new File("some_data_file2.dat"), td); Database.getCatalog().addTable(table2, "t2");
        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();
SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1"); SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");
        // create a filter for the where condition
Filter sf1 = new Filter(new Predicate(0,
Predicate.Op.GREATER_THAN, new IntField(1)), ss1);
JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1); Join j = new Join(p, sf1, ss2);
// and run it
try { j.open();
while (j.hasNext()) {
Tuple tup = j.next(); System.out.println(tup);
}
j.close(); Database.getBufferPool().transactionComplete(tid);
} catch (Exception e) { e.printStackTrace();
} }
}`

  Both tables have three integer fields. To express this, we create a TupleDesc object and pass it an array of Type objects indicating field types and String objects indicating field names. Once we have created this TupleDesc, we initialize two HeapFile objects representing the tables. Once we have created the tables, we add them to the Catalog. (If this were a database server that was already running, we would have this catalog information loaded; we need to load this only for the purposes of this test).
  Once we have finished initializing the database system, we create a query plan. Our plan consists of two SeqScan operators that scan the tuples from each file on disk, connected to a Filter operator on the first HeapFile, connected to a Join operator that joins the tuples in the tables according to the JoinPredicate. In general, these operators are instantiated with references to the appropriate table (in the case of SeqScan) or child operator (in the case of e.g., Join). The test program then repeatedly calls next on the Join operator, which in turn pulls tuples from its children. As tuples are output from the Join, they are printed out on the command line.
6


## Query Parser

There is a provided query parser for SimpleDB that you can use to write and run SQL queries against your database once you have completed the exercises in this lab.
The first step is to create some data tables and a catalog. Suppose you have a file data.txt with the following contents:
`1,10
2,20
3,30
4,40
5,50
5,50`
You can convert this into a SimpleDB table using the convert command (make sure to type ant first!): `java -jar dist/simpledb.jar convert data.txt 2 "int,int"`
This creates a file data.dat. In addition to the table’s raw data, the two additional parameters specify that each record has two fields and that their types are int and int.
Next, create a catalog file, catalog.txt, with the following contents: `data (f1 int, f2 int)`
This tells SimpleDB that there is one table, data (stored in data.dat) with two integer fields named f1 and f2. Finally, invoke the parser. You must run java from the command line (ant doesn’t work properly with interactive
targets.) From the simpledb/ directory, type:
`java -jar dist/simpledb.jar parser catalog.txt`
You should see output like:
`Added table : data with schema INT(f1), INT(f2),
SimpleDB>
Finally, you can run a query:
SimpleDB> select d.f1, d.f2 from data d; Started a new transaction tid = 1221852405823
ADDING TABLE d(data) TO tableMap
TABLE HAS tupleDesc INT(d.f1), INT(d.f2),
1       10
2       20
3       30
4       40
5       50
5       50
6 rows. ---------------- 0.16 seconds
SimpleDB>`

 The parser is relatively full featured (including support for SELECTs, INSERTs, DELETEs, and transactions), but does have some problems and does not necessarily report completely informative error messages. Here are some limita- tions to bear in mind:
• You must preface every field name with its tablename, even if the field name is unique (you can use table name aliases, as in the example above, but you cannot use the AS keyword.)
• Nested queries are supported in the WHERE clause, but not the FROM clause.
• No arithmetic expressions are supported (for example, you can’t take the sum of two fields.)
• At most one GROUP BY and one aggregate column are allowed.
• Set-oriented operators like IN, UNION, and EXCEPT are not allowed.
• Only AND expressions in the WHERE clause are allowed.
• UPDATE expressions are not supported.
• The string operator LIKE is allowed, but must be written out fully (that is, the Postgres tilde [~] shorthand is not allowed.)



