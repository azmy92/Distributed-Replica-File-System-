import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

import org.omg.CORBA.FREE_MEM;

public class ReplicaServer {

	public static final int FILE_ALREADY_EXIST = 2;
	public static final int SUCCESS = 1;
	public static final int FAIL = 0;
	public static final long COMMIT_TIME_OUT = 60 * 1000;
	public long startTime = System.currentTimeMillis();

	public HashMap<Long, Transaction> transactions = new HashMap<Long, Transaction>();
	public HashMap<String, Boolean> uncommitedFiles = new HashMap<String, Boolean>(); // Uncommitted
																						// files

	public String fullAdress;
	public String Address;
	public int Port;
	Registry registry; // RMI registry for lookup the remote objects.

	public ReplicaServer(int pport, String pAdress)
			throws InterruptedException, IOException {
		fullAdress = pAdress + "/" + pport;
		// INIT RMI
		initRmi(pport, pAdress);

	}

	private void initRmi(int pport, String pAdress) throws RemoteException {
		try {
			// get the address of this host.
			Address = (InetAddress.getLocalHost()).toString();
			System.out.println(Address);
		} catch (Exception e) {
			// throw new RemoteException("can't get inet address.");
			Address = pAdress;
		}
		Port = pport; // this port(registry port)
		System.out.println("replica address=" + Address + ",port=" + Port);
		System.setProperty("java.rmi.server.hostname", Address);
		try {
			System.setProperty("java.rmi.server.hostname", Address);
			RemoteServer rmi = new RemoteServer();

			registry = LocateRegistry.createRegistry(Port);
			registry.rebind("rmiserver", rmi);
		} catch (RemoteException e) {
			throw e;
		}
	}

	/**
	 * @param args
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public static void main(String[] args) throws InterruptedException,
			IOException {
		// TODO Auto-generated method stub

		ReplicaServer replica = new ReplicaServer(3558, "192.168.1.13");

		// try {
		// replica.connectToMaster(replica.Address, 1234);
		// } catch (NotBoundException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

	}

	public void connectToMaster(String serverAdd, int serverPort)
			throws InterruptedException, IOException, NotBoundException {

		ReplicaMaster rmiServer;
		Registry registry;
		// // get the registry
		registry = LocateRegistry.getRegistry(serverAdd, (new Integer(
				serverPort)).intValue());
		// look up the remote object
		rmiServer = (ReplicaMaster) registry.lookup("MASTER");
		System.out.println("connected to master server");

	}

	class RemoteServer extends java.rmi.server.UnicastRemoteObject implements
			RemoteReplicaServer, ReplicaServerClientInterface {

		private static final String MasterAddress = "192.168.1.13";
		private static final int MasterPort = 1234;
		private ReplicaMaster master;
		private boolean connectedToMaster = false;

		protected RemoteServer() throws RemoteException {
			super();
			// TODO Auto-generated constructor stub
		}

		@Override
		public synchronized int addNewFile(String fileName, boolean isPrimary)
				throws InterruptedException, IOException, NotBoundException {
			if (!connectedToMaster) {
				connectToMaster(MasterAddress, MasterPort);
				connectedToMaster = true;
			}

			File newFile = new File(fileName);
			if (fileExist(newFile))
				return FILE_ALREADY_EXIST;
			addFile(newFile);
			if (!uncommitedFiles.containsKey(fileName))
				uncommitedFiles.put(fileName, true);

			System.out.println("new file created un commited");

			// Propagate FILE to replicas if primary
			if (isPrimary) {

				try {
					String[] replicas = master
							.getReplicas(fileName, fullAdress);
					for (int i = 0; i < replicas.length; i++) {
						String[] currReplica = replicas[i].split("/");
						if (currReplica[0].equalsIgnoreCase(Address)
								&& currReplica[1].equalsIgnoreCase(Port + ""))
							continue;
						RemoteReplicaServer replica = connectToReplicasAsServer(
								currReplica[0],
								Integer.parseInt(currReplica[1]));
						int res = replica.addNewFile(fileName, false);
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return -2;
				}

			}

			return SUCCESS;
		}

		@Override
		public RemoteReplicaServer connectToReplicasAsServer(
				String replicaAddress, int replicaPort) throws RemoteException,
				NotBoundException {
			String serverAdd = replicaAddress;
			int serverPort = replicaPort;
			RemoteReplicaServer rmiServer;
			Registry registry;
			// // get the registry
			registry = LocateRegistry.getRegistry(serverAdd, (new Integer(
					serverPort)).intValue());
			// look up the remote object
			rmiServer = (RemoteReplicaServer) registry.lookup("rmiserver");
			return rmiServer;
		}

		private void addFile(File newFile) throws IOException {
			// BufferedWriter bf = new BufferedWriter(new FileWriter(newFile));
			// bf.flush();
			// bf.close();

		}

		private boolean fileExist(File file) {
			if (file.exists() && !file.isDirectory())
				return true;
			return false;
		}

		public void freeResources(long transactionId, String filename) {
			// TODO Auto-generated method stub
			System.out.println("FREEING resources of " + transactionId);
			Transaction trans = transactions.get(transactionId);
			trans.semaphore.release();
			trans.cancelJob();
			transactions.remove(transactionId);
			if (uncommitedFiles.containsKey(filename)) {
				uncommitedFiles.remove(filename);
				// TODO remove from uncommited file
			}
		}

		@Override
		public synchronized int initTransaction(long transactionId,
				String fileName, boolean isPrimary) throws RemoteException,
				InterruptedException, IOException, NumberFormatException,
				NotBoundException {
			System.out.println("itializing transaction " + transactionId);

			if (!connectedToMaster) {
				connectToMaster(MasterAddress, MasterPort);
				connectedToMaster = true;
			}

			// create new transaction
			Transaction transaction = new Transaction(transactionId, fileName);
			transactions.put(transactionId, transaction);
			transaction.semaphore.acquire(1);

			// schedule timeout task
			TimeTask task = new TimeTask();
			task.tID = transactionId;
			transaction.scheduleJob(task, COMMIT_TIME_OUT);

			// Propagate to replicas if primary
			if (isPrimary) {
				String[] replicas = master.getReplicas(fileName, fullAdress);
				for (int i = 0; i < replicas.length; i++) {
					String[] currReplica = replicas[i].split("/");
					if (currReplica[0].equalsIgnoreCase(Address)
							&& currReplica[1].equalsIgnoreCase(Port + ""))
						continue;
					RemoteReplicaServer replica = connectToReplicasAsServer(
							currReplica[0], Integer.parseInt(currReplica[1]));
					int res = replica.initTransaction(transactionId, fileName,
							false);

					if (res != SUCCESS)
						return FAIL;
				}
			}

			return SUCCESS;
		}

		@Override
		public void connectToMaster(String serverAdd, int serverPort)
				throws InterruptedException, IOException, NotBoundException {

			ReplicaMaster rmiServer;
			Registry registry;
			// // get the registry
			registry = LocateRegistry.getRegistry(serverAdd, (new Integer(
					serverPort)).intValue());
			// look up the remote object
			rmiServer = (ReplicaMaster) registry.lookup("rmiserver");
			master = rmiServer;
			System.out.println("connected to master server");

		}

		@Override
		public WriteMsg write(long txnID, long msgSeqNum, FileContent data)
				throws RemoteException, IOException {

			if (!connectedToMaster) {
				try {
					connectToMaster(MasterAddress, MasterPort);
					connectedToMaster = true;
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					System.out.println("error connecting to master");
					return null;
				}
			}

			long transactionId = txnID;
			long serialNum = msgSeqNum;
			String fData = data.data;

			System.out.println("-------write request------");
			System.out.println("transactionid: " + transactionId);
			System.out.println("serial number: " + serialNum);
			System.out.println("fData: " + fData);

			// validate transaction num
			if (!transactions.containsKey(transactionId)) {
				throw new RemoteException(
						"no recort related to this transaction id: "
								+ transactionId);
			}

			Transaction transaction = transactions.get(transactionId);

			// reset Timer
			transaction.cancelJob();
			TimeTask task = new TimeTask();
			task.tID = transactionId;
			transaction.scheduleJob(task, COMMIT_TIME_OUT);

			// update nummsg
			transaction.MesgSeq++;

			// update log
			transaction.appendLog("WRTIE", fData);

			// append data to buffer
			transaction.buffer[(int) msgSeqNum] = data.data;

			WriteMsg res = new WriteMsg();

			// Propagate writes to replicas
			if (data.isPrimary) {
				String[] replicas = master.getReplicas(transaction.file,
						fullAdress);
				for (int i = 0; i < replicas.length; i++) {
					try {
						String[] currReplica = replicas[i].split("/");
						if (currReplica[0].equalsIgnoreCase(Address)
								&& currReplica[1].equalsIgnoreCase(Port + ""))
							continue;
						ReplicaServerClientInterface replica = connectToReplicas(
								currReplica[0],
								Integer.parseInt(currReplica[1]));
						data.isPrimary = false;
						replica.write(txnID, msgSeqNum, data);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						System.out.println("replica " + i + " FAILED");
						res.fail = true;
						e.printStackTrace();
					}
				}
			}

			// return result
			ReplicaLoc loc = new ReplicaLoc();
			loc.location = Address;
			res.loc = loc;
			res.timeStamp = System.currentTimeMillis() - startTime;
			res.transactionId = transactionId;

			return res;

		}

		@Override
		public FileContent read(String fileName) throws FileNotFoundException,
				IOException, RemoteException {

			File file = new File(fileName);
			if (!fileExist(file))
				throw new FileNotFoundException(
						"the requested file doesn't exist in the replica");
			if (uncommitedFiles.containsKey(fileName))
				// file is being updated
				throw new FileNotFoundException(
						"the requested file is not commited yet");

			// read file and append in a string
			StringBuilder out = new StringBuilder();
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			String line = br.readLine();
			while (line != null) {
				line = line + "\n";
				out.append(line.toCharArray(), 0, line.length());
				line = br.readLine();
			}

			br.close();
			FileContent fc = new FileContent();
			fc.data = out.toString().substring(0, out.toString().length() - 1);
			return fc;

		}

		@Override
		public int commit(long txnID, long numOfMsgs)
				throws MessageNotFoundException, RemoteException {

			long transactionId = txnID;

			// validate transaction number
			if (!transactions.containsKey(transactionId))
				throw new RemoteException(
						"no recort related to this transaction id: "
								+ transactionId);

			Transaction transaction = transactions.get(transactionId);

			if (transaction.MesgSeq != numOfMsgs) {
				transaction.MesgSeq -= 1;
				for (int i = 0; i < transaction.MesgSeq + 1; i++) {
					System.out.println("BUFFER: " + transaction.buffer[i]);
					if (transaction.buffer[i] == ""
							|| transaction.buffer[i] == null) {
						System.out.println("Dropped message detected, index: "
								+ i);
						System.out.println("notifing the client");
						return i;
					}
				}
			}

			// ///////////////////critical region////////////////////////////
			// ensure sequetial consistency
			Transaction olderTrans = getPreviousTransaction(transaction.file,
					transactionId);
			if (olderTrans != null)
				try {
					olderTrans.semaphore.acquire(1);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return -2;
				}
			try {
				transaction.flushFile();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return -2;
			}
			// /////////////////////////////////////////////////////////////////////

			transaction.appendLog("COMMIT", "-----------------");
			freeResources(transactionId, transaction.file);
			// Propagate commits to replicas

			try {
				String[] replicas = master.getReplicas(transaction.file,
						fullAdress);
				// you are not primary
				if (replicas == null)
					return -1;
				for (int i = 0; i < replicas.length; i++) {
					String[] currReplica = replicas[i].split("/");
					if (currReplica[0].equalsIgnoreCase(Address)
							&& currReplica[1].equalsIgnoreCase(Port + ""))
						continue;
					ReplicaServerClientInterface replica = connectToReplicas(
							currReplica[0], Integer.parseInt(currReplica[1]));
					int res = replica.commit(transactionId, numOfMsgs);
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -2;
			}

			return -1;

		}

		private Transaction getPreviousTransaction(String file, long tid) {

			int diffrence = Integer.MAX_VALUE;
			Transaction res = null;
			for (Long key : transactions.keySet()) {
				Transaction current = transactions.get(key);
				if (!current.file.equals(file))
					continue;
				if (current.Tid < tid && (tid - current.Tid) < diffrence) {
					res = current;
					diffrence = (int) (tid - current.Tid);
				}
			}
			return res;
		}

		@Override
		public boolean abort(long txnID) throws RemoteException {
			// validate
			if (!transactions.containsKey(txnID))
				return false;
			Transaction transaction = transactions.get(txnID);
			freeResources(txnID, transaction.file);
			System.out.println("transaction: " + txnID + " ABORTED");

			try {
				String[] replicas = master.getReplicas(transaction.file,
						fullAdress);
				if (replicas == null)
					return true;
				for (int i = 0; i < replicas.length; i++) {
					String[] currReplica = replicas[i].split("/");
					if (currReplica[0].equalsIgnoreCase(Address)
							&& currReplica[1].equalsIgnoreCase(Port + ""))
						continue;
					ReplicaServerClientInterface replica = connectToReplicas(
							currReplica[0], Integer.parseInt(currReplica[1]));
					boolean res = replica.abort(txnID);
				}

			} catch (Exception e) {
				return false;
			}

			return true;
		}

		class TimeTask extends TimerTask {

			long tID;
			String fileName;

			// kill uncommited transaction
			@Override
			public void run() {
				// TODO Auto-generated method stub

				System.out.println("Transaction commit time out ID: " + tID);
				freeResources(tID, fileName);

			}

		}

		@Override
		public boolean isAlive() throws RemoteException {
			return true;
		}

		@Override
		public String[] getFiles() throws RemoteException {
			System.out.println("MASTER REQUESTED LIST OF FILES");
			File folder = new File("Root");
			File[] listOfFiles = folder.listFiles();
			String[] res = new String[listOfFiles.length];

			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					System.out.println("File " + listOfFiles[i].getName());
					res[i] = "Root/" + listOfFiles[i].getName();
				} else if (listOfFiles[i].isDirectory()) {
					System.out.println("Directory " + listOfFiles[i].getName()
							+ " but subs are not supported..ignore");
				}
			}
			return res;
		}

		public ReplicaServerClientInterface connectToReplicas(
				String replicaAddress, int replicaPort) throws RemoteException,
				InterruptedException, IOException, NotBoundException {
			// TODO Auto-generated method stub
			String serverAdd = replicaAddress;
			int serverPort = replicaPort;
			ReplicaServerClientInterface rmiServer;
			Registry registry;
			// // get the registry
			registry = LocateRegistry.getRegistry(serverAdd, (new Integer(
					serverPort)).intValue());
			// look up the remote object
			rmiServer = (ReplicaServerClientInterface) registry
					.lookup("rmiserver");
			return rmiServer;
			// System.out.println("connected");
		}

	}
}