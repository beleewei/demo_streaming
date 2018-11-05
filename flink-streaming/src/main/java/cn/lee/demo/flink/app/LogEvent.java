package cn.lee.demo.flink.app;

/**
 * Project Name:flink-parent
 * Package Name:org.apache.flink.streaming.examples.kafka.app
 * ClassName: LogEvent &lt;br/&gt;
 * date: 2018/6/28 11:19 &lt;br/&gt;
 * TODO  详细描述这个类的功能等
 *
 * @author LI WEI
 * @since JDK 1.6
 */
public class LogEvent {
	private String id;
	private String productId;
	private String productVersion;
	private String channelId;
	private String dealTime;
	private String imei;
	private long time;
	private int count;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getProductId() {
		return productId;
	}

	public void setProductId(String productId) {
		this.productId = productId;
	}

	public String getProductVersion() {
		return productVersion;
	}

	public void setProductVersion(String productVersion) {
		this.productVersion = productVersion;
	}

	public String getChannelId() {
		return channelId;
	}

	public void setChannelId(String channelId) {
		this.channelId = channelId;
	}

	public String getDealTime() {
		return dealTime;
	}

	public void setDealTime(String dealTime) {
		this.dealTime = dealTime;
	}

	public String getImei() {
		return imei;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public String toString() {
		return id+"|"+productId+"|"+productVersion+"|"+channelId+"|"+imei+"|"+dealTime+"|"+time+"|"+count;
	}
}
