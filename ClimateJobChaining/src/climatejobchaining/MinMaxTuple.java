/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatejobchaining;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author ekta23
 */
public class MinMaxTuple implements Writable {

    private double minavgtemp;
    private double maxavgtemp;
    private String minCountry;
    private String maxCountry;

    public String getMaxCountry() {
        return maxCountry;
    }

    public void setMaxCountry(String maxCountry) {
        this.maxCountry = maxCountry;
    }

    public String getMinCountry() {
        return minCountry;
    }

    public void setMinCountry(String minCountry) {
        this.minCountry = minCountry;
    }

    public MinMaxTuple() {
        minavgtemp = 0.0;
        maxavgtemp = 0.0;
    }

    public double getMaxavgtemp() {
        return maxavgtemp;
    }

    public void setMaxavgtemp(double maxavgtemp) {
        this.maxavgtemp = maxavgtemp;
    }

    public double getMinavgtemp() {
        return minavgtemp;
    }

    public void setMinavgtemp(double minavgtemp) {
        this.minavgtemp = minavgtemp;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        if (in.readUTF().length() > 1 && in.readDouble() > 0.0) {
            minavgtemp = in.readDouble();
            maxavgtemp = in.readDouble();
            minCountry = in.readUTF();
            maxCountry = in.readUTF();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(minCountry);
        out.writeUTF(maxCountry);
        out.writeDouble(minavgtemp);
        out.writeDouble(maxavgtemp);
    }
    
    @Override
    public String toString(){
        return minCountry +" " + maxCountry+" " + minavgtemp +" "+ maxavgtemp;
    }
    
}