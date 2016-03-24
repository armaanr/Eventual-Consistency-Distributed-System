import java.net.*;
import java.util.*;
import java.io.*;

public class Server extends Thread
{
   //accepts connections
   public ServerSocket serverSocket;
   
   //contains the information about all processes
   public Map<Integer, ProcessInfo> replicas;
   public Map<Integer, ProcessInfo> clients;
   public int maxClient;
   public String outputFileName;
   public int min_delay;
   public int max_delay;
   
   public Server(int port) throws IOException
   {
      this.serverSocket = new ServerSocket(port);
      this.replicas = new HashMap<Integer, ProcessInfo>();
      this.clients = new HashMap<Integer, ProcessInfo>();
      this.outputFileName = "outputs/output_log";
      this.maxClient = 0;
   }
   
   //Parses the given config file to initialize the Server variables
   public void readConfig(File file) throws IOException
   {
	   @SuppressWarnings("resource")
	   Scanner scanner = new Scanner(file);
	   
	   //sets up delays
	   if(scanner.hasNext())
	   {
		   String[] delays = scanner.nextLine().split(" ");
		   this.min_delay = Integer.parseInt(delays[0]);
		   this.max_delay = Integer.parseInt(delays[1]);
	   }

	   //populates hashmap containing information for all replicas
	   while(scanner.hasNext())
	   {
		    String[] tokens = scanner.nextLine().split(" ");
		    
		    int id = Integer.parseInt(tokens[0]);
		    InetAddress ip = InetAddress.getByName(tokens[1]);
	        int port = Integer.parseInt(tokens[2]); 
	           
		    ProcessInfo procInfo = new ProcessInfo(ip, port);
		    
		    replicas.put(id, procInfo);    
	    }
	   
	    //sets the name of the output file
	    int currProcess = processIdentifier(this.serverSocket.getLocalPort());
	    String processName = Integer.toString(currProcess).concat(".txt"); 
	   	this.outputFileName = this.outputFileName.concat(processName);
	   
	   	System.out.println(outputFileName);
	    
   }
   
   

   //parses received messages and accordingly allocates tasks
   public void receiver() throws IOException
   {
	   
	   System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
       Socket server = this.serverSocket.accept();
       int clientId = -1;
       
       System.out.println(server.getRemoteSocketAddress());
       
       clientId = clientAdder(server);
       
       System.out.println("clientId = " + clientId);
       
       //receives message
       DataInputStream in = new DataInputStream(server.getInputStream());
       String message = "";
       message = in.readUTF();   
       message = "d";
       char[] cmd = message.toCharArray();
       char cmdType = cmd[0];
       
       
       //handles 'send' commands from command line
       if(cmdType == 'p')
       {
    	   putHandler(cmd, clientId);   
    	  
       }
       else if(cmdType == 'g')
       {
    	   getHandler(cmd, clientId); 
       }
       else if(cmdType == 'd')
       {
    	   dumpHandler(cmd, clientId); 
       }
       
       server.close();
       
   }

   //Collects client information and adds it to the client list
   private int clientAdder(Socket server) throws UnknownHostException, IOException 
   {
	   String clientSockAdd = server.getRemoteSocketAddress().toString();
	   
       String[] clientInfo = clientSockAdd.split(":");
       StringBuilder sb = new StringBuilder(clientInfo[0]);
       sb.deleteCharAt(0);
       
       InetAddress clientIP = InetAddress.getByName(sb.toString());
       int clientPort = Integer.parseInt(clientInfo[1]);
       ProcessInfo client = new ProcessInfo(clientIP, clientPort);
       
       int clientId = clientIdentifier(clientPort);
       if(clientId == -1)
       {
    	   System.out.println("max Client = "+ maxClient);
    	   clientId = maxClient;
    	   clients.put(clientId, client);
           maxClient++;
       }
       
       System.out.println("client size = "+ clients.size());
       return clientId;
   }
   
   //Handles the 'put' command from the client and updates the output file.
   private void putHandler(char[] cmd,int clientId)
   {
	   try 
	   { 
		   //prepare put log info
		   String logLine = "666,";
		   String intId = Integer.toString(clientId);
		   String variable = Character.toString(cmd[1]);
		   String value = Character.toString(cmd[2]);
		   
		   logLine = logLine.concat(intId+",put,"+variable+","+System.currentTimeMillis());
		   String logLineReq = logLine.concat(",req,"+value+"\n");
		   String logLineResp = logLine.concat(",resp,"+value+"\n");
		   
		   Writer output = new BufferedWriter(new FileWriter(outputFileName, true));
		   output.append(logLineReq);
		   output.close();
		   
		   Responder responder = new Responder(logLineResp, min_delay, max_delay, clients.get(clientId), outputFileName);
		   responder.start();
		   
	   } 
	   catch (IOException e) 
	   {
		e.printStackTrace();
	   }
	   
   }
   
   private void getHandler(char[] cmd, int clientId)
   {
	   try 
	   { 
		   //prepare put log info
		   String logLine = "666,";
		   String intId = Integer.toString(clientId);
		   String variable = Character.toString(cmd[1]);
		   
		   logLine = logLine.concat(intId+",get,"+variable+","+ System.currentTimeMillis());
		   String logLineReq = logLine.concat(",req"+"\n");
		   
		   Writer output = new BufferedWriter(new FileWriter(outputFileName, true));
		   output.append(logLineReq);
		   output.close();
		   
		   int value = findGetValue(variable);
		   
		   String logLineResp = logLine.concat(",resp,"+value+"\n");
		   
		   Responder responder = new Responder(logLineResp, min_delay, max_delay, clients.get(clientId), outputFileName);
		   responder.start();
		   
	   } 
	   catch (IOException e) 
	   {
		e.printStackTrace();
	   }
   }

   private int findGetValue(String variable) throws FileNotFoundException 
   {
	   int value = -1;
	   @SuppressWarnings("resource")
	   Scanner scanner = new Scanner(new File(outputFileName));
	   
	   while(scanner.hasNextLine())
	   {
		   String[] curr = scanner.nextLine().split(",");
		   
		   if(curr[2].equals("put") && curr[3].equals(variable))
		   {
			   value = Integer.parseInt(curr[6]);
		   }
	   }
	   
	   return value;
   }
   
   private void dumpHandler(char[] cmd ,int clientId) throws FileNotFoundException
   {
	   @SuppressWarnings("resource")
	   Scanner scanner = new Scanner(new File(outputFileName));
	   
	   while(scanner.hasNextLine())
	   {
		   System.out.println(scanner.nextLine());
	   }
	   
	   Responder responder = new Responder("", min_delay, max_delay, clients.get(clientId), outputFileName);
	   responder.start();
   }

  //returns the id of a process using it's port number
  private int processIdentifier(int sourcePort) 
  {
	  int src = 0;
	  for(Integer i : replicas.keySet())
	  {	
		  if(replicas.get(i).getPort() == sourcePort)
		  {
			  src = i;
			  break;
		  }
	  }
	
	  return src;
  }
  
  //returns the ID of the client at the given port number
  private int clientIdentifier(int sourcePort) 
  {
		int src = -1; 
		
		System.out.println("sourcePort = " + sourcePort);
		for(Integer i : clients.keySet())
		{
			System.out.println("ID = "+ i);
			if(clients.get(i).getPort() == sourcePort)
			{
				System.out.println("ports "+clients.get(i).getPort() );
				src = i;
				break;
			}
		}
		
		System.out.println("src =" + src);
		return src;
  }
   

   
   public void run()
   {
      while(true)
      {
         try
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
        	
            receiver();
            
         }catch(SocketTimeoutException s)
         {
            System.out.println("Socket timed out!");
            break;
         }catch(IOException e)
         {
            e.printStackTrace();
            break;
         }
         
         
      }
   }
   
  public static void main(String [] args) throws IOException
  { 
	  
      try
      {
    	 int port = Integer.parseInt(args[0]);
    	  
         
         //starts the server
    	 Server reciever = new Server(port);
         String fileName = args[1];
         File file = new File(fileName);
         
         reciever.readConfig(file);
         reciever.start();
         
         //handles input from command line
         InetAddress add = reciever.serverSocket.getInetAddress(); 
         CommandTaker listen = new CommandTaker(add, port);
         
         listen.start();
       
      }catch(IOException e)
      {
         e.printStackTrace();
      }
   }
}
