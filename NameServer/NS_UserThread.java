import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Inet4Address;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;

public class NS_UserThread extends Thread {
	NameServer nameServer = null;
	static Socket socket = null;
	static ObjectOutputStream outputStream = null;
	static ObjectInputStream inputStream = null;
	static HashMap<Integer, String> data = new HashMap<>();
	static NameServerInfo nsInfo = null;
	 static Socket fwdSocket = null;
	 int port;
	 String root_ip;
	 int root_port;
	 int id;
	public NS_UserThread(int Id,int Port, String Ip, int Root_port,NameServer nameserver) {
		this.port=Port;
		this.root_ip=Ip;
		this.root_port=Root_port;
		this.id=Id;
		this.nameServer=nameserver;
	}
	// TODO Auto-generated constructor stub
	@Override
	public void run() {
		try  {
			String userCmd="";
			NameServer.nsInfo =new NameServerInfo(id, port);
			while(!userCmd.equalsIgnoreCase("quit")) {
				
				Scanner sc = new Scanner(System.in);
				System.out.println("Command : ");
				userCmd = sc.nextLine();	
				String[] input = userCmd.split(" ", 2);
				switch(input[0]) {
				case "enter":
					socket = new Socket(root_ip, root_port);	
					outputStream = new  ObjectOutputStream(socket.getOutputStream());
					inputStream = new ObjectInputStream(socket.getInputStream());
					String nameServerIP = Inet4Address.getLocalHost().getHostAddress();
					outputStream.writeObject("entry "+id + " "+ nameServerIP + " " +port);
					//ns will send its id, its ip and its listeningport where other server can contact it for key
					String serverTracker = (String) inputStream.readObject();
					int successorPortListning = (int) inputStream.readObject();
					int predessorPortListning = (int) inputStream.readObject();
					int successorId = (int) inputStream.readObject();
					int predessorId = (int) inputStream.readObject();
					String successorIP = (String) inputStream.readObject();	
					String predessorIP = (String) inputStream.readObject();
					
					nameServer.nsInfo.updateInformation(successorPortListning,predessorPortListning, successorId, predessorId, successorIP, predessorIP);
					nameServer.nsInfo.id = id;
					//System.out.println("SuccessorId : " + successorId +" PredessorId " +predessorId + "PredessorIP " + predessorIP+" PredessorPort : "+predessorPortListning);
					while(true) {
						
						int key =  (int) inputStream.readObject();
						if(key == -1)
							break;
						
						String value = (String) inputStream.readObject();
						nameServer.table.put(key, value);
					}
					outputStream.close();
					inputStream.close();
					socket.close();
					System.out.println("Successful entry");
					System.out.println("Range of IDs managed ["+predessorId+","+id+"]");
					System.out.println("Servers Visited" + serverTracker);
				

					
					break;

				case "exit":
					
					socket = new Socket(nameServer.nsInfo.successorIP, nameServer.nsInfo.successorPortListning);
					outputStream = new  ObjectOutputStream(socket.getOutputStream());
					inputStream = new ObjectInputStream(socket.getInputStream());
					
					outputStream.writeObject("updateYourPredessorAndTakeAllKeys");
					outputStream.writeObject(nameServer.nsInfo.predessorPortListning);
					outputStream.writeObject(nameServer.nsInfo.predessorId);
					outputStream.writeObject(nameServer.nsInfo.predessorIP);
					
					for(int key = nameServer.nsInfo.predessorId; key < nameServer.nsInfo.id; key++) {
						if(nameServer.table.containsKey(key)) {
							//System.out.println(key);
							outputStream.writeObject(key);
							outputStream.writeObject(nameServer.table.get(key));
							nameServer.table.remove(key);
							
						}
					}
					outputStream.writeObject(-1);
					outputStream.close();
					inputStream.close();
					socket.close();
					
					socket = new Socket(nameServer.nsInfo.predessorIP, nameServer.nsInfo.predessorPortListning);
					outputStream = new  ObjectOutputStream(socket.getOutputStream());
					inputStream = new ObjectInputStream(socket.getInputStream());
					
					outputStream.writeObject("updateYourSuccessor");
					outputStream.writeObject(nameServer.nsInfo.successorPortListning);
					outputStream.writeObject(nameServer.nsInfo.successorId);
					outputStream.writeObject(nameServer.nsInfo.successorIP);
					
					outputStream.close();
					inputStream.close();
					socket.close();
					socket = new Socket(root_ip, root_port);
					outputStream = new  ObjectOutputStream(socket.getOutputStream());
					outputStream.writeObject("updateMaxServerID");
					outputStream.writeObject(id);
					socket.close();
					System.out.println("Successful exit");
					System.out.println("Range of IDs handed over ["+nameServer.nsInfo.predessorId+","+id+"]");
					System.out.println("NameServer SuccessorId : "+nameServer.nsInfo.successorId);
					
					break;
				}
				 
				
			}
			System.out.println("NameServer Exited");
		}catch(Exception e) {
			e.printStackTrace();
		}
	}


}