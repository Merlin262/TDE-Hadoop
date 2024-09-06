package TDE.CustomWritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TransactionAverageWritable implements Writable {

    private float totalValue;
    private int transactionCount;

    public TransactionAverageWritable() {}

    public TransactionAverageWritable(float totalValue, int transactionCount) {
        this.totalValue = totalValue;
        this.transactionCount = transactionCount;
    }

    public float getTotalValue() {
        return totalValue;
    }

    public void setTotalValue(float totalValue) {
        this.totalValue = totalValue;
    }

    public int getTransactionCount() {
        return transactionCount;
    }

    public void setTransactionCount(int transactionCount) {
        this.transactionCount = transactionCount;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(totalValue);
        out.writeInt(transactionCount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        totalValue = in.readFloat();
        transactionCount = in.readInt();
    }
}