import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

public class DNS_Root {
	public static HashMap<Integer,String> table =new HashMap<>();
	private static ServerSocket server;
	static Socket fwdSocket = null;
	private static int port;
	static int count = 0;
	static Socket socket = null;
	ObjectOutputStream outputStream = null;
	ObjectInputStream inputStream = null;
	static ArrayList<Integer> nsIds = new ArrayList<>();
	
	
	RNameServerInfo nsInfo;
	public DNS_Root(){
		nsInfo = new RNameServerInfo(0,port);
		nsIds.add(0);
	}
	String lookup(int key) throws UnknownHostException, IOException, ClassNotFoundException {
		String value="";
		if(table.containsKey(key)) {
			System.out.println("\nSequence of Server IDs Visited: 0" );
			System.out.println("Value retrived from Server ID 0" );
			value="The value at location is: "+table.get(key);
		}
		else if(nsInfo.predessorId==0 && nsInfo.successorId==0) {
			value="\n"+"Key Not Found!!"+"\n";
		}else {
		 fwdSocket = new Socket(nsInfo.getSuccessorIP(), nsInfo.successorPortListning);
		 ObjectInputStream inputStreamFwd = new ObjectInputStream(fwdSocket.getInputStream());
		 ObjectOutputStream outputStreamFwd = new ObjectOutputStream(fwdSocket.getOutputStream());
		 outputStreamFwd.writeObject("lookup "+key);
		 outputStreamFwd.writeObject("0");
		 value = (String) inputStreamFwd.readObject();
		 String serverTracker = (String) inputStreamFwd.readObject();
		 int count = 0;
		 if(value.equalsIgnoreCase("Nokey")) {
			 count++;
			 value="No Key Found";
		 }
				 
		 for(int i = 0; i < serverTracker.length(); i++)
		 {
			 if(serverTracker.charAt(i) == '-')
				 count++;
		 }
		 Collections.sort(nsIds);
		 int last_Id=0;
			System.out.print("Sequence of Server IDs Visited: " );
		 for(int id : nsIds) {
			 if(count-1 < 0) {
				 System.out.println(id);
				 last_Id=id;
			 }
			 	else
				 System.out.print(id + "->");
				
			 count--;
			 if(count< 0)
				 break;
		 }
		 System.out.println("Value retrived from Server ID "+last_Id );
		 fwdSocket.close();
		 value="The value at location is: "+value;
		}
		
		 return value;
		
	}
	void insert(int key, String value) throws IOException, ClassNotFoundException {
		//check if the key should be in bootstrap
		if(key > Collections.max(nsIds)) {
			System.out.println("\nServer Visited 0"  );
			System.out.println("Key Inserted at 0\n"  );
			table.put(key,value);
		}
			
		else {
		//if no then contact successor
		 fwdSocket = new Socket(nsInfo.getSuccessorIP(), nsInfo.successorPortListning);
		 ObjectInputStream inputStreamFwd = new ObjectInputStream(fwdSocket.getInputStream());
		 ObjectOutputStream outputStreamFwd = new ObjectOutputStream(fwdSocket.getOutputStream());
		 outputStreamFwd.writeObject("Insert "+key+" "+value);
		
		 String serverTracker = (String) inputStreamFwd.readObject();
		 int count = 0;
		 int last_Id=0;
		 for(int i = 0; i < serverTracker.length(); i++)
		 {
			 if(serverTracker.charAt(i) == '-')
				 count++;
		 }
		 Collections.sort(nsIds);
			System.out.print("\nServer Visited : "  );
		 for(int id : nsIds) {
			 if(count-1 < 0) {
				 System.out.println(id);
				 last_Id=id;
			 }
			 else
				 System.out.print(id + "->");
				
			 count--;
			 if(count< 0)
				 break;
		 }
		 System.out.println("Key Inserted at "+last_Id+"\n");
		 fwdSocket.close();
		}
	}
	void delete(int key) throws UnknownHostException, IOException, ClassNotFoundException {
		
		//if key in bootstrap server then dekete
		if(key > Collections.max(nsIds)) {
			System.out.println("\nDeletion Succesful");
			System.out.println("Server Visited 0\n"  );
			table.remove(key);
		}
			
		else {
		 fwdSocket = new Socket(nsInfo.getSuccessorIP(), nsInfo.successorPortListning);
		 ObjectInputStream inputStreamFwd = new ObjectInputStream(fwdSocket.getInputStream());
		 ObjectOutputStream outputStreamFwd = new ObjectOutputStream(fwdSocket.getOutputStream());
		 outputStreamFwd.writeObject("delete "+key);

		 String serverTracker = (String) inputStreamFwd.readObject();
		 int count = 0;boolean notFound = false;
		 for(int i = 0; i < serverTracker.length(); i++)
		 {
			 if(serverTracker.charAt(i) == '-')
				 count++;
			 else if(serverTracker.charAt(i) == 'N')
				 notFound = true;
		 }
		 if(notFound)
			 System.out.println("\nKey Not Found");
		 else
			 System.out.println("\nDeletion Succesful");
		 Collections.sort(nsIds);
			System.out.print("Server Visited : "  );
		 for(int id : nsIds) {
			 if(count-1 < 0)
				 System.out.println(id);
			 else
				 System.out.print(id + "->");
				
			 count--;
			 if(count< 0)
				 break;
		 } 
		fwdSocket.close();
		}
	}

	public static void main(String[] args) throws NumberFormatException, IOException {
		// TODO Auto-generated method stub
		try{
			BufferedReader reader = new BufferedReader(new FileReader((String)args[0]));
		
		int ID = Integer.parseInt(reader.readLine());

		String Port = reader.readLine();
		 port=Integer.parseInt(Port);
		DNS_Root bootstrap= new DNS_Root();
		String data=reader.readLine();
		while(null!=data) {
			String[]keyValue= data.split(" ");
			int key=Integer.parseInt(keyValue[0]);
			String value=keyValue[1];
			table.put(key, value);
			data=reader.readLine();
		}
		System.out.println("DNS Root starting..");
		server = new ServerSocket(port);
		int maxServerID = 0;
		DNS_UserThread cThread = new DNS_UserThread(bootstrap);
		cThread.start();
		while(true) {
			socket = server.accept();
			//System.out.println("added new NameServer");
			ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
			ObjectOutputStream outputStream = new ObjectOutputStream(socket.getOutputStream());
			String nameServerDetails = (String) inputStream.readObject();
			String[] nameServerDetailsStr = nameServerDetails.split(" ");
			//System.out.println(nameServerDetailsStr[0]);
			int newNSId = 0;
			int newNSListeningPort = 0;
			String newNSIP = "";
			if(!nameServerDetailsStr[0].equals("updateYourPredessorAndTakeAllKeys")) {
				if( !nameServerDetailsStr[0].equals("updateYourSuccessor") && !nameServerDetailsStr[0].equals("updateMaxServerID") ) {
					newNSId = Integer.parseInt(nameServerDetailsStr[1]);
					newNSIP = nameServerDetailsStr[2];
					newNSListeningPort = Integer.parseInt(nameServerDetailsStr[3]);
				}
			
			}
			switch(nameServerDetailsStr[0]) {
			case "entry":
				bootstrap.nsIds.add(newNSId);
				Collections.sort(bootstrap.nsIds);
				String serverTracker = "0";
				for(int visitedId : bootstrap.nsIds)
					if(visitedId < newNSId)
						serverTracker.concat("->"+visitedId);

				outputStream.writeObject(serverTracker);
				if(bootstrap.nsInfo.getSuccessorId() == 0)//if only one server intial
				{
					outputStream.writeObject(bootstrap.nsInfo.serverPortForConnection);//succssor port
					outputStream.writeObject(bootstrap.nsInfo.serverPortForConnection);//predessor port
					outputStream.writeObject(bootstrap.nsInfo.id);//sucessor id
					outputStream.writeObject(bootstrap.nsInfo.id);//predessor id
					outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress());//successor ip
					outputStream.writeObject(Inet4Address.getLocalHost().getHostAddress());//predessor ip
					bootstrap.nsInfo.updateInformation(newNSListeningPort, newNSListeningPort, newNSId, newNSId, newNSIP, newNSIP);

					//give all the value from 0 to id
					for(int key = 0; key < newNSId; key++) {

						if(bootstrap.table.containsKey(key)) {
							//System.out.println(key);
							outputStream.writeObject(key);
							outputStream.writeObject(bootstrap.table.get(key));
							bootstrap.table.remove(key);
						}

					}
					outputStream.writeObject(-1);
				}
				else if(maxServerID < newNSId) {

					//System.out.println("Server with greates value");
					bootstrap.nsInfo.predessorId = newNSId; 
					int nextServerListeningPort = bootstrap.nsInfo.successorPortListning;
					String nextServerIP = bootstrap.nsInfo.getSuccessorIP();

					fwdSocket = new Socket(nextServerIP, nextServerListeningPort);

					ObjectInputStream inputStreamFwd = new ObjectInputStream(fwdSocket.getInputStream());
					ObjectOutputStream outputStreamFwd = new ObjectOutputStream(fwdSocket.getOutputStream());
					outputStreamFwd.writeObject("entryAtLast "+newNSId + " "+ newNSIP + " " + newNSListeningPort);

					int successorPortListning = (int) inputStreamFwd.readObject();
					outputStream.writeObject(successorPortListning);
					int predessorPortListning = (int) inputStreamFwd.readObject();
					outputStream.writeObject(predessorPortListning);
					int successorId = (int) inputStreamFwd.readObject();
					outputStream.writeObject(successorId);
					int predessorId = (int) inputStreamFwd.readObject();
					outputStream.writeObject(predessorId);
					String successorIP = (String) inputStreamFwd.readObject();	
					outputStream.writeObject(successorIP);
					String predessorIP = (String) inputStreamFwd.readObject();	
					outputStream.writeObject(predessorIP);


					for(int key = maxServerID; key < newNSId; key++) {

						if(bootstrap.table.containsKey(key)) {
							//System.out.println(key);
							outputStream.writeObject(key);
							outputStream.writeObject(bootstrap.table.get(key));
							bootstrap.table.remove(key);
						}
					}
					outputStream.writeObject(-1);
				}
				else {

					int nextServerListeningPort = bootstrap.nsInfo.successorPortListning;
					String nextServerIP = bootstrap.nsInfo.getSuccessorIP();

					if(bootstrap.nsInfo.getSuccessorId() > newNSId)
						bootstrap.nsInfo.updateInformation(newNSListeningPort,bootstrap.nsInfo.predessorPortListning, newNSId, bootstrap.nsInfo.predessorId, newNSIP,bootstrap.nsInfo.predessorIP);

					fwdSocket = new Socket(nextServerIP, nextServerListeningPort);

					ObjectInputStream inputStreamFwd = new ObjectInputStream(fwdSocket.getInputStream());
					ObjectOutputStream outputStreamFwd = new ObjectOutputStream(fwdSocket.getOutputStream());
					outputStreamFwd.writeObject("entry "+newNSId + " "+ newNSIP + " " + newNSListeningPort);

					int successorPortListning = (int) inputStreamFwd.readObject();
					outputStream.writeObject(successorPortListning);
					int predessorPortListning = (int) inputStreamFwd.readObject();
					outputStream.writeObject(predessorPortListning);						 
					int successorId = (int) inputStreamFwd.readObject();
					outputStream.writeObject(successorId);
					int predessorId = (int) inputStreamFwd.readObject();
					outputStream.writeObject(predessorId);
					String successorIP = (String) inputStreamFwd.readObject();	
					outputStream.writeObject(successorIP);
					String predessorIP = (String) inputStreamFwd.readObject();	
					outputStream.writeObject(predessorIP);
					while(true) {

						int key =  (int) inputStreamFwd.readObject();
						outputStream.writeObject(key);
						if(key == -1)
							break;

						String value = (String) inputStreamFwd.readObject();
						outputStream.writeObject(value);

					}

					fwdSocket.close();
					//System.out.println("done");
				}


				outputStream.close();
				inputStream.close();
				socket.close();

				break;
			case "updateYourPredessorAndTakeAllKeys":
				//System.out.println("In Successor to updateYourPredessorAndTakeAllKeys");
				
				int predessorPortListning = (int) inputStream.readObject();//update successor port
				int predessorId = (int) inputStream.readObject();//update successor id
				String predessorIP = (String) inputStream.readObject();//update successor ip
				bootstrap.nsInfo.updateInformation(bootstrap.nsInfo.successorPortListning, predessorPortListning, bootstrap.nsInfo.getSuccessorId(), predessorId,bootstrap.nsInfo.successorIP,predessorIP);
				while(true) {
					
					int key =  (int) inputStream.readObject();
					if(key == -1)
						break;
					
					String value = (String) inputStream.readObject();
					bootstrap.table.put(key, value);
					//System.out.println("Key : "+key+" Value : "+value);
					
				}
				//System.out.println("Updated Informatio successorId" + bootstrap.nsInfo.successorId);
				
				break;
			
			case "updateYourSuccessor":
				//System.out.println("In Predessor to updateYourSuccessor");
				int successorPort = (int) inputStream.readObject();//update predessor port
				int successorId = (int) inputStream.readObject();//update predessor id
				String successorIP = (String) inputStream.readObject();//update predessor ip
				bootstrap.nsInfo.updateInformation(successorPort, bootstrap.nsInfo.predessorPortListning,successorId, bootstrap.nsInfo.predessorId, successorIP,bootstrap.nsInfo.predessorIP);
			break;
			
			case "updateMaxServerID":
				int exitedID = (int) inputStream.readObject();
				bootstrap.nsIds.remove(Integer.valueOf(exitedID));
				//System.out.println("UpdatingMaxServerID..");
				break;
			
			}
			maxServerID = Collections.max(bootstrap.nsIds);
			//System.out.println("BOOTSTRAP SuccessorId : "+bootstrap.nsInfo.successorId + " PredessorId :"+bootstrap.nsInfo.predessorId);
			


		}
	}catch(Exception e) {
		e.printStackTrace();
	}
	}
}
	
