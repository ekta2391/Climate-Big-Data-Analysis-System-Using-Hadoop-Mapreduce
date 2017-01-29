/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package climatedataanalysis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author ekta23
 */
public class MinMaxTuple implements Writable{
    private double minAvgTemp;
    private double maxAvgtemp;

    public double getMaxAvgtemp() {
        return maxAvgtemp;
    }

    public void setMaxAvgtemp(double maxAvgtemp) {
        this.maxAvgtemp = maxAvgtemp;
    }

    public double getMinAvgTemp() {
        return minAvgTemp;
    }

    public void setMinAvgTemp(double minAvgTemp) {
        this.minAvgTemp = minAvgTemp;
    }
    
    
    public MinMaxTuple(){
        super();
    }
    @Override
    public void write(DataOutput out) throws IOException {
    out.writeDouble(minAvgTemp);
    out.writeDouble(maxAvgtemp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
    minAvgTemp = new Double(in.readDouble());
    maxAvgtemp = new Double(in.readDouble());
    }
    
    public String toString(){
        return minAvgTemp + "\t"+ maxAvgtemp;
    }
    
}
