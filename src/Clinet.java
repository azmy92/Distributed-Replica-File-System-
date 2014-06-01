import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;

public class Clinet {

	private MasterServerClientInterface master;
	private ReplicaServerClientInterface currentReplica;
	private ArrayList<String> msgBuf = new ArrayList<String>();
	private long msgSeq = 0;

	public Clinet(String MasterAdress, int MasterPort) throws RemoteException,
			NotBoundException {
		connectToMaster(MasterAdress, MasterPort);

	}

	private void connectToMaster(String masterAdress, int masterPort)
			throws RemoteException, NotBoundException {
		// TODO Auto-generated method stub
		String serverAdd = masterAdress;
		int serverPort = masterPort;
		MasterServerClientInterface rmiServer;
		Registry registry;
		// // get the registry
		registry = LocateRegistry.getRegistry(serverAdd, (new Integer(
				serverPort)).intValue());
		// look up the remote object
		rmiServer = (MasterServerClientInterface) registry.lookup("rmiserver");
		master = rmiServer;
		// System.out.println("connected");
	}

	private void connectToReplica(String replicaAdress, int replicaPort)
			throws RemoteException, NotBoundException {
		// TODO Auto-generated method stub
		String serverAdd = replicaAdress;
		int serverPort = replicaPort;
		ReplicaServerClientInterface rmiServer;
		Registry registry;
		// // get the registry
		registry = LocateRegistry.getRegistry(serverAdd, (new Integer(
				serverPort)).intValue());
		// look up the remote object
		rmiServer = (ReplicaServerClientInterface) registry.lookup("rmiserver");
		currentReplica = rmiServer;
		// System.out.println("connected");
	}

	public ReplicaLoc[] requestFileLocation(String fileName)
			throws FileNotFoundException, RemoteException, IOException {
		ReplicaLoc[] locs = master.read(fileName);
		return locs;
	}

	// save the file into the root directory
	public void Read(ReplicaLoc loc, String Dir, String fileName)
			throws NumberFormatException, NotBoundException,
			FileNotFoundException, IOException {
		String[] add = loc.location.split("/");
		connectToReplica(add[0], Integer.parseInt(add[1]));
		FileContent filec = currentReplica.read(fileName);
		File dir = new File(Dir);
		File actualFile = new File(Dir, fileName);
		BufferedWriter br = new BufferedWriter(new FileWriter(actualFile));
		br.write(filec.data);
		br.flush();
		br.close();

	}

	public WriteMsg requestWrite(String fileName) throws RemoteException,
			IOException, NumberFormatException, NotBoundException {
		msgSeq = 0;
		FileContent fc = new FileContent();
		fc.data = fileName;
		WriteMsg msg = master.write(fc);
		String[] add = msg.loc.location.split("/");
		connectToReplica(add[0], Integer.parseInt(add[1]));
		return msg;
	}

	int i = 0;

	public void writeAppend(WriteMsg wmsg, String data) throws RemoteException,
			IOException {
		long tId = wmsg.transactionId;
		FileContent fdata = new FileContent();
		fdata.data = data;
		fdata.isPrimary = true;
		msgBuf.add(fdata.data);
		if (i != 2)
			currentReplica.write(tId, msgSeq, fdata);
		++msgSeq;
		++i;
	}

	public int commit(WriteMsg wmsg) throws MessageNotFoundException,
			IOException {
		int res = 0;
		while (true) {
			res = currentReplica.commit(wmsg.transactionId, msgSeq);
			if (res != -1) {
				String msg = msgBuf.get(res);
				long tId = wmsg.transactionId;
				FileContent fdata = new FileContent();
				fdata.data = msg;
				msgBuf.add(fdata.data);
				currentReplica.write(tId, res, fdata);
				++msgSeq;
			} else
				break;
			System.out.println("droped message detected index " + res);
		}
		return res;
	}

	public void abort(WriteMsg wmsg) throws RemoteException {
		msgSeq = 0;
		currentReplica.abort(wmsg.transactionId);
	}

	public static void main(String[] args) throws IOException,
			NotBoundException, MessageNotFoundException, InterruptedException {
		System.setProperty("java.rmi.server.hostname", "192.168.1.13");
		Clinet c = new Clinet("192.168.1.13", 1234);

		// /////////write on an existing file then reading it///////////////

		// WriteMsg wmsg = c.requestWrite("Root/test1.txt");
		// c.writeAppend(wmsg, "TIMO LOVERS");
		// c.writeAppend(wmsg, "AMR DIAB ROCKS");
		// c.commit(wmsg);
		//
		// ReplicaLoc[] locs = c.requestFileLocation("Root/test1.txt");
		// c.Read(locs[0], "download", "Root/test1.txt");

		// /////////////////////////////////////////////////////

		// //////////Writing new file then reading it/////////

		WriteMsg wmsg = c.requestWrite("Root/c95.txt");
		c.writeAppend(wmsg, "m0-ddd-");
		c.writeAppend(wmsg, "m1");
		//Thread.sleep(20000);
		c.writeAppend(wmsg, "m2");
		c.writeAppend(wmsg, "m3");
		 //c.msgSeq = 3;
		System.out.println(c.commit(wmsg));
		//
		 ReplicaLoc[] locs = c.requestFileLocation("Root/c95.txt");
		 c.Read(locs[0], "download", "Root/c95.txt");

		// ////////////////////////////////////////

	}

}
