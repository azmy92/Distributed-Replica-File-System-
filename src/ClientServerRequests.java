import java.io.File;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ClientServerRequests extends Remote {

	/**
	 * 1) The client requests a new transaction ID from the server. The request
	 * includes the name of the file to be muted during the transaction. 2) The
	 * server generates a unique transaction ID and returns it, a time-stamp,
	 * and the location of the primary replica of that file to the client in an
	 * acknowledgment to the client's file update request. 3) If the file
	 * specified by the client does not exist, the server creates the file on
	 * the replicaServers and initializes its meta-data.
	 * 
	 * @param fileName
	 * @return string array s[0] = time-stamp, s[1] = primary replica location,
	 *         s[2] = unique transaction id
	 * @throws RemoteException
	 * @throws InterruptedException
	 * @throws IOException 
	 */
	public String[] requireId(String fileName) throws RemoteException,
			InterruptedException, IOException;


}
