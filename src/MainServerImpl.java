import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RemoteServer;
import java.util.ArrayList;
import java.util.HashMap;

public class MainServerImpl {

	public static Integer COMITTED = 0;
	public static Integer UN_COMITTED = 1;
	public static Integer STATUS_FAIL = 0;

	public int currentTransactionId = 0;
	public String Address;
	public int Port;
	public BufferedReader metaDatabr;
	public String metaFileName = "metadata.txt";
	public String TransMetaFile = "transMeta.txt";

	public HashMap<String, String> primaryReplicas = new HashMap<String, String>();
	public HashMap<String, ArrayList<String>> fileReplicasMap = new HashMap<String, ArrayList<String>>();

	public RemoteReplicaServer[] replicas;
	public String[] replicasAdress;
	public String address = "192.168.1";
	long startTime = System.currentTimeMillis();

	protected MainServerImpl() throws RemoteException {
		super();
		// TODO Auto-generated constructor stub
	}

	public MainServerImpl(String address, int port) throws IOException,
			InterruptedException {

		Address = address;
		Port = port;

		metaDatabr = new BufferedReader(new FileReader(metaFileName));

		initReplicas();

		System.setProperty("java.rmi.server.hostname", address);

		initRmi(Port, Address);

	}

	Registry registry; // RMI registry for lookup the remote objects.

	private void initRmi(int pport, String pAdress) throws IOException {
		try {
			// get the address of this host.
			Address = (InetAddress.getLocalHost()).toString();
			System.out.println(Address);
		} catch (Exception e) {
			// throw new RemoteException("can't get inet address.");
			Address = pAdress;
		}
		Port = pport; // this port(registry port)
		System.out.println("this address=" + Address + ",port=" + Port);
		try {
			System.setProperty("java.rmi.server.hostname", Address);
			RemoteMaster rmi = new RemoteMaster();
			registry = LocateRegistry.createRegistry(Port);
			registry.rebind("rmiserver", rmi);
		} catch (RemoteException e) {
			throw e;
		}
	}

	/**
	 * reads replica addresses from metadata file connect to replicas initiate
	 * fileReplica MAP initiate PrimaryReplica MAP
	 * 
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void initReplicas() throws IOException, InterruptedException {
		BufferedReader transIdMeta = new BufferedReader(new FileReader(
				TransMetaFile));
		currentTransactionId = Integer.parseInt(transIdMeta.readLine());
		transIdMeta.close();
		String line = metaDatabr.readLine();
		int replicaNum = Integer.parseInt(line);
		replicas = new RemoteReplicaServer[replicaNum];
		replicasAdress = new String[replicaNum];
		line = metaDatabr.readLine();
		int counter = 0;
		while (line != null && !line.equalsIgnoreCase(" ")) {
			String replicaAdress = line;
			String[] replicaFiles;
			if (connect(replicaAdress, counter)) {
				replicaFiles = replicas[counter].getFiles();
				updateFileReplicaMap(replicaFiles, replicaAdress);
				++counter;
			}
			line = metaDatabr.readLine();
		}
		metaDatabr.close();

		System.out.println(primaryReplicas.toString());
	}

	private void updateFileReplicaMap(String[] replicaFiles,
			String replicaAdress) {
		for (int i = 0; i < replicaFiles.length; i++) {
			ArrayList<String> mapReplicas;
			if (fileReplicasMap.containsKey(replicaFiles[i])) {
				mapReplicas = fileReplicasMap.get(replicaFiles[i]);
			} else {
				mapReplicas = new ArrayList<String>();
			}
			mapReplicas.add(replicaAdress);
			fileReplicasMap.put(replicaFiles[i], mapReplicas);

			// set primary replicas
			if (!primaryReplicas.containsKey(replicaFiles[i])) {
				primaryReplicas.put(replicaFiles[i], replicaAdress);
			}
		}

	}

	private boolean connect(String key, int pointer)
			throws InterruptedException, IOException {
		System.out.println("trying to connect to " + key);
		// TODO Auto-generated method stub
		boolean success = false;
		// TODO create replica server

		RemoteReplicaServer rmiServer;
		Registry registry;
		String[] vals = key.split("/");
		String serverAdd = vals[0];
		int serverPort = Integer.parseInt(vals[1]);
		try {
			// // get the registry
			registry = LocateRegistry.getRegistry(serverAdd, (new Integer(
					serverPort)).intValue());
			// look up the remote object

			rmiServer = (RemoteReplicaServer) registry.lookup("rmiserver");
			replicas[pointer] = rmiServer;
			success = true;
			System.out.println("connected");
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (success)
			replicasAdress[pointer] = key;
		return success;

	}

	private synchronized int getNewId() throws IOException {
		int newId = currentTransactionId;
		++currentTransactionId;

		// write new id to file
		PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(
				TransMetaFile, false)));
		writer.println(currentTransactionId + "");
		writer.flush();
		writer.close();
		return newId;
	}

	private synchronized double getTimeStamp() {
		return System.currentTimeMillis() - startTime;
	}

	private int findReplicaFromAddress(String primaryAdress) {
		for (int i = 0; i < replicasAdress.length; i++) {
			if (replicasAdress[i].equalsIgnoreCase(primaryAdress))
				return i;
		}
		return -1;
	}

	/*
	 * int[0] = primary replica address int[1] = second replica add int[2] =
	 * third replica add
	 */
	private int[] getRandReplica(String fileName) throws InterruptedException,
			IOException {

		int rand = (int) (Math.random() * 10) % replicas.length;
		System.out.println(rand);
		int rand2 = rand;
		int rand3 = rand;

		if (replicas.length >= 3) {
			while (rand2 == rand) {
				rand2 = (int) (Math.random() * 10) % replicas.length;
				System.out.println("rand2: " + rand2);
			}

			while (rand3 == rand || rand3 == rand2) {
				rand3 = (int) (Math.random() * 10) % replicas.length;
				System.out.println("rand3: " + rand3);

			}
		}

		int[] res = { rand, rand2, rand3 };
		return res;
	}

	/*
	 * add new created file in MAPS
	 */
	private synchronized void addMetaData(int[] rand, String fileName)
			throws IOException {
		primaryReplicas.put(fileName, replicasAdress[rand[0]]);
		ArrayList<String> reps = new ArrayList<String>();
		for (int i = 0; i < rand.length; i++) {
			String replAdd = replicasAdress[rand[i]];
			reps.add(replAdd);
			if (replicasAdress.length < 3)
				break;
		}
		fileReplicasMap.put(fileName, reps);
		System.out.println("MAPS UPDATED");
		System.out.println("Primary replica map" + primaryReplicas.toString());
		System.out.println("fileReplicaMap: " + fileReplicasMap.toString());
	}

	private synchronized String exists(String fileName) {
		// TODO Auto-generated method stub
		if (!primaryReplicas.containsKey(fileName))
			return null;
		String primaryReplica = primaryReplicas.get(fileName);
		return primaryReplica;
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		MainServerImpl server = new MainServerImpl("192.168.1.13", 1234);
		// String[] res = server.requireId("Root/test.txt");
		// /System.out.println(server.replicas[0].read(11, 0));
		// server.replicas[0].write(13, 1, "newdata 3", true);
		// server.replicas[0].commit(13, true);
	}

	class RemoteMaster extends java.rmi.server.UnicastRemoteObject implements
			MasterServerClientInterface, ReplicaMaster {

		protected RemoteMaster() throws RemoteException {
			super();
			// TODO Auto-generated constructor stub
		}

		@Override
		public synchronized ReplicaLoc[] read(String fileName)
				throws FileNotFoundException, IOException, RemoteException {
			System.out
					.println("client querying file " + fileName + " Location");
			System.out.println(primaryReplicas.toString());
			if (!fileReplicasMap.containsKey(fileName))
				throw new FileNotFoundException(
						"the requested file doesn't exist");
			ArrayList<String> replcs = fileReplicasMap.get(fileName);
			// TODO check liveness
			ReplicaLoc[] locs = new ReplicaLoc[replcs.size()];
			for (int i = 0; i < locs.length; i++) {
				locs[i] = new ReplicaLoc();
				locs[i].location = replcs.get(i);
			}
			return locs;
		}

		@Override
		public WriteMsg write(FileContent data) throws RemoteException,
				IOException {
			String fileName = data.data;
			String[] ret = null;
			try {
				ret = requireId(fileName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			WriteMsg res = new WriteMsg();
			ReplicaLoc loc = new ReplicaLoc();
			loc.location = ret[2];
			res.loc = loc;
			res.timeStamp = (long) Float.parseFloat(ret[0]);
			res.transactionId = Long.parseLong(ret[1]);

			return res;
		}

		public String[] requireId(String fileName) throws InterruptedException,
				IOException, NumberFormatException, NotBoundException {
			System.out.println("client requires new transaction id");
			String[] res = new String[3];
			int id = getNewId();
			double timeStamp = getTimeStamp();
			res[0] = timeStamp + "";
			res[1] = id + "";
			System.out.println("new id " + res[1]);
			System.out.println("time stamp: " + res[0]);
			String primaryAdress = exists(fileName);
			int[] rand = null;
			if (primaryAdress != null) {
				System.out.println("file exists");
				res[2] = primaryAdress;
			} else {
				System.out.println("file don't exist");
				while (true) {
					System.out.println("setting new primary replica");
					rand = getRandReplica(fileName);
					if (rand == null)
						continue;
					else {
						res[2] = replicasAdress[rand[0]];
						break;
					}
				}
				addMetaData(rand, fileName);

				int status = replicas[rand[0]].addNewFile(fileName, true);
				System.out.println(status);
				if (status != RemoteReplicaServer.SUCCESS
						&& status != RemoteReplicaServer.FILE_ALREADY_EXIST) {
					System.out.println("Replica " + replicasAdress[rand[0]]
							+ " failed to create new file " + fileName);
				} else {
					System.out.println("new file created");
				}

			}

			int index = findReplicaFromAddress(res[2]);
			replicas[index].initTransaction(id, fileName, true);
			System.out.println("res = " + res[0] + " " + res[1] + " " + res[2]);
			return res;
		}

		@Override
		public String[] getReplicas(String fileName, String address)
				throws FileNotFoundException, IOException {

			//System.out.println("replica "+address+" requesting list of file replicas");
			String primary = primaryReplicas.get(fileName);
			//System.out.println("primary is "+primary);
			if (!primary.equalsIgnoreCase(address))
				return null;
			ReplicaLoc[] locs = read(fileName);
			String[] res = new String[locs.length];
			for (int i = 0; i < res.length; i++) {
				res[i] = locs[i].location;
			}
			return res;
		}

	}
}