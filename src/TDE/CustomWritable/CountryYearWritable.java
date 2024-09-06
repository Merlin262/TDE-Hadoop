package TDE.CustomWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CountryYearWritable implements WritableComparable<CountryYearWritable> {
    private String country;
    private String year;

    public CountryYearWritable() {}

    public CountryYearWritable(String country, String year) {
        this.country = country;
        this.year = year;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(country));
        out.writeUTF(String.valueOf(year));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
       country = in.readUTF();
       year = in.readUTF();
    }

    @Override
    public int compareTo(CountryYearWritable o) {
        if(this.hashCode() < o.hashCode()) {
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return 1;
        }
        return 0;
    }
    @Override
    public int hashCode() {
        return Objects.hash(country, year);
    }

@Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountryYearWritable that = (CountryYearWritable) o;
        return country.equals(that.country) && year.equals(that.year);
    }

    @Override
    public String toString() {
        return country + "," + year;
    }
}