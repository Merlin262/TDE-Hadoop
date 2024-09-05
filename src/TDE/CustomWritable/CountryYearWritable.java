package TDE.CustomWritable;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

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
        WritableUtils.writeString(out, country);
        WritableUtils.writeString(out, year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        country = WritableUtils.readString(in);
        year = WritableUtils.readString(in);
    }

    @Override
    public int compareTo(CountryYearWritable o) {
        int cmp = country.compareTo(o.country);
        if (cmp != 0) {
            return cmp;
        }
        return year.compareTo(o.year);
    }

    @Override
    public int hashCode() {
        return country.hashCode() * 163 + year.hashCode();
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
        return country + ";" + year;
    }
}