package cn.lee.demo.flink.watermark;

import java.io.Serializable;

/**
 * Project Name:demo_streaming
 * Package Name:cn.lee.demo.flink.watermark
 * ClassName: Animal &lt;br/&gt;
 * date: 2018/11/14 14:23 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class Animal implements Serializable {
    private String name;
    private String eventTime;

    public Animal(String name, String eventTime) {
        this.name = name;
        this.eventTime = eventTime;
    }

    public String getEventTime() {
        return eventTime;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "<"+this.name+","+this.getEventTime()+">";
    }
}
