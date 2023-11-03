package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIerator implements DbFileIterator{

    private static final long serialVersionUID = 1L;
    private TransactionId tid;

    private HeapFile hf;

    private  int pgNo;
    private int tableId;

    private Iterator<Tuple> itr;

    public HeapFileIerator(TransactionId tid,HeapFile hf) {
        this.tid=tid;
        this.hf=hf;
        tableId=hf.getId();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        pgNo=0;
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(tableId,pgNo), Permissions.READ_ONLY);
        itr=page.iterator();
        pgNo++;
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(itr==null){
            return false;
        }
        while(!itr.hasNext()){
            if(pgNo<hf.numPages()){
                HeapPage page =  (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(tableId,pgNo), Permissions.READ_ONLY);
                itr=page.iterator();
                pgNo++;
            }else{
                return false;
            }
        }
        return true;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(itr==null){
            throw new NoSuchElementException();
        }
        return itr.next();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
       itr=null;
    }
}
