import java.util.HashMap;
import java.util.Scanner;

public class DNS_UserThread extends Thread {
	DNS_Root root = null;
	public DNS_UserThread(DNS_Root root) {
		this.root = root;
	}
		// TODO Auto-generated constructor stub
	@Override
	public void run() {
		try  {
			String userCmd="";
			while(!userCmd.equalsIgnoreCase("quit")) {
				Scanner sc = new Scanner(System.in);
				System.out.println("COMMAND : ");
				userCmd = sc.nextLine();	
				String[] input = userCmd.split(" ", 2);
				switch(input[0]) {
				case "lookup":
					if(Integer.parseInt(input[1])>1023)
					{
						System.out.println("Plese enter key value in range 0 to 1023");
						break;
					}
					String value=root.lookup(Integer.parseInt(input[1]));
					System.out.println(value+"\n");
					break;
					
				case "insert":
					
					String[] keyValue=input[1].split(" ");
					if(Integer.parseInt(keyValue[0])>1023)
					{
						System.out.println("Plese enter key value in range 0 to 1023");
						break;
					}
					root.insert(Integer.parseInt(keyValue[0]), keyValue[1]);
					break;
					
				case "delete":
					if(Integer.parseInt(input[1])>1023)
					{
						System.out.println("Plese enter key value in range 0 to 1023");
						break;
					}
					root.delete(Integer.parseInt(input[1]));
					
					break;
					
				}
					
				
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}


}
