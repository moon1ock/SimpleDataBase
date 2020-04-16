package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock = new Byte((byte) 0);
    private TransactionId dirty;

    /**
     * Create a HeapPage from a set of bytes of data read from disk. The format of a
     * HeapPage is a set of header bytes indicating the slots of the page that are
     * in use, some number of tuple slots. Specifically, the number of tuples is
     * equal to:
     * <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p>
     * where tuple size is the size of tuples in this database table, which can be
     * determined via {@link Catalog#getTupleDesc}. The number of 8-bit header words
     * is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     * 
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     * 
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        int tupleSize = td.getSize();
        int numTuples = (int) Math.floor((BufferPool.getPageSize() * 8) / (tupleSize * 8 + 1));
        return numTuples;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each
     * tuple occupying tupleSize bytes
     * 
     * @return the number of bytes in the header of a page in a HeapFile with each
     *         tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {

        // some code goes here
        return (int) Math.ceil((double) getNumTuples() / 8);
    }

    /**
     * Return a view of this page before it was modified -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            // should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page. Used to
     * serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte array
     * generated by getPageData to the HeapPage constructor and have it produce an
     * identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i = 0; i < header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); // - numSlots *
                                                                                                 // td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty HeapPage.
     * Used to add new, empty pages to the file. Passing the results of this method
     * to the HeapPage constructor will create a HeapPage with no valid tuples in
     * it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; // all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should
     * be updated to reflect that it is no longer stored on any page.
     *
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        // 1) check whether the tuple page id matches the id of the heap page
        if (!this.pid.equals(t.getRecordId().getPageId()))
            throw new DbException("You are attempting to delete a tuple that is not on the given Heap Page");
        else {
            int tuplenum = t.getRecordId().getTupleNumber();
            // check whether the tuple with such number exists and can actually be deleted
            if (!isSlotUsed(tuplenum))
                throw new DbException("You are attempting to delete a tuple that is not on the given Heap Page");
            else
                markSlotUsed(tuplenum, false); // mark the tuple fasle
        }
        return;

    }

    /**
     * Adds the specified tuple to the page; the tuple should be updated to reflect
     * that it is now stored on this page.
     * 
     * @throws DbException if the page is full (no empty slots) or tupledesc is
     *                     mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        // Yes, we could use getNumEmptySlots() and append to the tuple to the end of
        // the page but that would cause fragmentation and headache with cleaning later.
        // I, Andriy, propose a solution of first checking whether there are any empty slots
        // but then linearly scanning the heap page to find the first appropriate spots for the
        // tuple. This would reduce fragmentation, and the solution is O(2n) time complexity
        // i.e. worse only by a constant factor in terms of time, but more efficienct in
        // terms of memory usage

        // check if the page is full
        if (this.getNumEmptySlots() <= 0)
            throw new DbException("Heap Page you are trying to insert a tuple to is full");

        // iterate over the slots to find an empty one
        for (int i = 0; i < numSlots; i++) {
            if (!isSlotUsed(i)) {
                markSlotUsed(i, true);
                t.setRecordId(new RecordId(pid, i));
                tuples[i] = t;
                return;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction that did the
     * dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        // save the transaction that was last dirtied for the next funciton
        if (dirty)
            this.dirty = tid;
        else
            this.dirty = null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if
     * the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return this.dirty; //
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int emptySlots = this.numSlots;
        for (int i = 0; i < this.numSlots; i++) {
            if (isSlotUsed(i))
                emptySlots--;
        }
        return emptySlots;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        if (i < 0 || i / 8 >= header.length)
            return false;
        return (this.header[i / 8] & (1 << (i % 8))) > 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1

        // will have to deal with individual bits here

        // each slot is 1 byte, int(i / 8) will give us a slot
        // and i % 8 will give us the exact bit position to the
        // bit we want to mark

        if (value) // start with marking the slot as used, i.e. flip the headerbit to 1
            this.header[i / 8] = (byte) (this.header[i / 8] | (1 << (i % 8))); // use & here in case a used slot is
                                                                               // being marked as used
        else
            this.header[i / 8] = (byte) (this.header[i / 8] & ~(1 << (i % 8))); // flip the bit to 0 otherwise
        // System.out.println("header");
        // System.out.println(this.header[i/8]);
        // if(this.header[i/8] != -1){
        // System.out.println(i);
        // System.out.println("header");
        // System.out.println(this.header[i/8]);
        // System.out.println(i%8);
        // }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this
     *         iterator throws an UnsupportedOperationException) (note that this
     *         iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        ArrayList<Tuple> usedTuples = new ArrayList<Tuple>();

        for (int i = 0; i < numSlots; i++) {
            if (isSlotUsed(i))
                usedTuples.add(tuples[i]);
        }

        return usedTuples.iterator();
    }

}
