import java.io.Serializable;


public class WriteMsg implements Serializable {
	public long transactionId;
	public  long timeStamp;
	public ReplicaLoc loc;
	public long ack;
	public boolean fail = false;

}
