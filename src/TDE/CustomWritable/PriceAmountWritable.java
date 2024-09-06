package TDE.CustomWritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PriceAmountWritable implements Writable {
    long price;
    float amount;

    public PriceAmountWritable() {
    }

    public PriceAmountWritable(long price, float amount   ) {
        this.price = price;
        this.amount = amount;
    }

    public float getAmount() {
        return amount;
    }

    public void setAmount(float amount) {
        this.amount = amount;
    }

    public long getPrice() {
        return price;
    }

    public void setPrice(long price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(price);
        out.writeFloat(amount);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        price = in.readLong();
        amount = in.readFloat();
    }
}
