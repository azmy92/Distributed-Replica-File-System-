import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RemoteReplicaServer extends Remote {
	
	public static final String NO_EXIST = "NO EXIST";
	public static final String UN_COMMITED = "NO commited yet";
	public static final int FILE_ALREADY_EXIST = 2;
	public static final int SUCCESS = 1;
	public static final int FAIL = 0;

	
	public int initTransaction(long transactionId,String fileName, boolean isPrimary)throws RemoteException, InterruptedException, IOException, NumberFormatException, NotBoundException;

	public int addNewFile(String fileName,boolean isPrimary) throws RemoteException, InterruptedException, IOException, NotBoundException;
	
	
	public boolean isAlive()throws RemoteException;
	
	public String[] getFiles()throws RemoteException;

	void connectToMaster(String address, int port) throws InterruptedException,
			IOException, NotBoundException;

	public RemoteReplicaServer connectToReplicasAsServer(String replicaAddress,
			int replicaPort) throws RemoteException, NotBoundException;
	


//	/**
//	 * Files are read entirely. When a client sends a file name to the server,
//	 * the entire file is returned to the client.
//	 * 
//	 * @param transactionId
//	 *            , unique id of the transaction
//	 * @param serialNum
//	 *            , unique number for transaction
//	 * @return Entire File
//	 * @throws RemoteException
//	 * @throws InterruptedException
//	 * @throws FileNotFoundException 
//	 * @throws IOException 
//	 */
//	public String read(int transactionId, int serialNum) throws RemoteException,
//			InterruptedException, FileNotFoundException, IOException;
//
//	/**
//	 * The client sends to the replicaServer a series of write requests to the
//	 * file specified in the transaction. Each request has a unique serial
//	 * number. The server appends all writes sent by the client to the file.
//	 * Updates are also propagated in the same order to other replicaServers.
//	 * 
//	 * 1-server append data to current transaction file 2-propagate update to
//	 * replicas 3-keep track of transactions (transaction log)
//	 * 
//	 * @param serialNum
//	 *            unique number for transaction
//	 * @param data
//	 *            data to be appended
//	 * @param transactionId
//	 *            unique id of the transaction
//	 * @return status code
//	 * @throws RemoteException
//	 * @throws InterruptedException
//	 * @throws IOException 
//	 */
//	public int write(int transactionId, int serialNum, String data,boolean isPrimary)
//			throws RemoteException, InterruptedException, IOException;
//
//	/**
//	 * 1- send commit to all replica servers to flush data on disk 2- receive
//	 * all acks from replicas 3- send ack to client
//	 * 
//	 * @return status code
//	 * @throws RemoteException
//	 * @throws InterruptedException
//	 */
//	public int commit(int transactionId, boolean isPrimar) throws RemoteException, InterruptedException;
//
//	/**
//	 * A client can decide to abort the transaction after it has started. Note,
//	 * that the client might have already sent write requests to the server. In
//	 * this case, the client requests transaction abort from the server. The
//	 * client's abort request is handled as follows: 1) The primary
//	 * replicaServer ensures that no data that the client had sent as part of
//	 * transaction is written to the file on the disk of any of the
//	 * replicaServers. 2) If a new file was created as part of that transaction,
//	 * the master server deletes the file from its metadata and the file is
//	 * deleted from all replicaServers. 3) The primary replicaServer
//	 * acknowledges the client's abort.
//	 * 
//	 * @param transactionId
//	 * @param serialNum
//	 * @return status code
//	 * @throws RemoteException
//	 * @throws InterruptedException
//	 */
//	public int abort(int transactionId, int serialNum) throws RemoteException,
//			InterruptedException;
}
