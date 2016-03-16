import java.net.*;
import java.util.*;
import java.io.*;

//handles input from command line
public class CommandTaker extends Thread {
	
	public static InetAddress sendAddress;
	public static int sendPort;
	
	public CommandTaker(InetAddress add, int portNum) throws IOException
	{
		sendAddress = add;
		sendPort = portNum;
	}
	
	//parses commands and executes the applicable function
	public static void listen() throws IOException
	{
		@SuppressWarnings("resource")
		Scanner scanner = new Scanner(new InputStreamReader(System.in));
		String line = scanner.nextLine();

		  
		   String[] tokens = line.split(" ");
	       
	       if(tokens[0].equals("send") && (tokens.length >= 3))
	       {		    	 
	    	  sendParsedCommand(line);		    	  
	       }
	       else if(tokens[0].equals("msend") && (tokens.length >= 2))
	       {
	    	  msendParsedCommand(line);
	       }
	       else
	       {
	    	   System.out.println("Command not found");
	       }

	}
	
	//parses 'msend' command entered through command line
	private static void msendParsedCommand(String line) throws IOException 
	{
			Socket client = new Socket(sendAddress, sendPort);
			line = line.concat(" mcmd");
			
			DataOutputStream out = new DataOutputStream(client.getOutputStream());
			out.writeUTF(line);
	   	    
			client.close();
			
	}


	//parses 'send' command entered through command line
	private static void sendParsedCommand(String line) throws IOException 
	{
		Socket client = new Socket(sendAddress, sendPort);
		line = line.concat(" cmd");
		
		DataOutputStream out = new DataOutputStream(client.getOutputStream());
		out.writeUTF(line);
   	    
		client.close();
		
	}

	public void run()
	   {
	      while(true)
	      {
	    	  if (Thread.interrupted()) 
	    	  {
		    	    try {
						throw new InterruptedException();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		      }
	    	  
	    	  try 
	    	  {
	    		
				listen();
	    	  } catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
	    	  }
	    	  
	    	
	    	 
	    	  
	    	  
	      }
	   }

}
