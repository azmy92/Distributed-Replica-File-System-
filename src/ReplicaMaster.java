import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.Remote;
import java.rmi.RemoteException;


public interface ReplicaMaster extends Remote{
	/**
	 * return replicas containing file
	 * @param fileName
	 * @return Addresses of replicas containing file
	 * @throws RemoteException
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public String[] getReplicas(String fileName,String address)  throws RemoteException, FileNotFoundException, IOException;

}
