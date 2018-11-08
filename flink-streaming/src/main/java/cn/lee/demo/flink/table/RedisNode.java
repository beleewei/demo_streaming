package cn.lee.demo.flink.table;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Project Name:demo_streaming
 * Package Name:cn.lee.demo.flink.table
 * ClassName: RedisNode &lt;br/&gt;
 * date: 2018/11/6 8:15 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class RedisNode {
    private long time;
    private int tps;
    private double inputBytes;
    private double outputBytes;
    private long size;
    private String role;
    private String address;


    public long getTime() {
        return time;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public int getTps() {
        return tps;
    }

    public void setTps(int tps) {
        this.tps = tps;
    }

    public double getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(double inputBytes) {
        this.inputBytes = inputBytes;
    }

    public double getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(double outputBytes) {
        this.outputBytes = outputBytes;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    @Override
    public String toString() {
        return address+"|"+this.getRole()+"|"+this.inputBytes+"|"+this.outputBytes+" at "+this.tps+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(time));
    }
}
