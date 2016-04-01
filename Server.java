import java.net.*;
import java.util.*;
import java.io.*;

public class Server extends Thread
{
   //accepts connections
   public ServerSocket serverSocket;
   
   //contains the information about all processes
   public int id;
   public Map<Integer, ProcessInfo> replicas;
   public Map<Integer, ProcessInfo> clients;
   public int maxClient;
   public String outputFileName;
   public int min_delay;
   public int max_delay;
   
   //for linearizability
   public TotalOrderMulticast linearizability;
   public boolean sequencer;
   public int globalSequence;
   
   public Server(int port) throws IOException
   {
      this.serverSocket = new ServerSocket(port);
      this.replicas = new HashMap<Integer, ProcessInfo>();
      this.clients = new HashMap<Integer, ProcessInfo>();
      this.outputFileName = "outputs/output_log";
      this.maxClient = 0;
      this.min_delay= 0;
      this.max_delay = 0;
      
      this.linearizability = new TotalOrderMulticast();
      this.sequencer =  false;
      this.globalSequence = 0;
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
	    this.id = processIdentifier(this.serverSocket.getLocalPort());
	    String processName = Integer.toString(this.id).concat(".txt"); 
	   	this.outputFileName = this.outputFileName.concat(processName);
	   	
	   	if(this.id == 1)
	   	{
	   		this.sequencer = true;
	   	}
	   	
	    
   }

   //parses received messages and accordingly allocates tasks
   public void receiver() throws IOException
   {
	   
	   System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
       Socket server = this.serverSocket.accept();
       System.out.println(server.getLocalSocketAddress());
       
       //receives message
       DataInputStream in = new DataInputStream(server.getInputStream());
       String message = "";
       message = in.readUTF();  
       
       System.out.println(server.getRemoteSocketAddress());
       System.out.println("message rcvd => ("+ message+")");
       
       String[] clientMessage = message.split(" ");
       
       if(clientMessage[0].equals("TO"))
       {
    	   System.out.println("checking TO messages");
    	   
    	   String currMsg = clientMessage[2];
    	   int currId = Integer.parseInt(clientMessage[3]);
    	   int currgs = Integer.parseInt(clientMessage[4]);
    	   TotalOrderMulticast.TotalOrderInfo curr = linearizability.new TotalOrderInfo(currMsg, currId, currgs);
    	   
    	   if(clientMessage[1].equals("M"))
    	   {
    		   if(linearizability.seqBuffer.contains(curr))
    		   {
    			   int currIndex = linearizability.seqBuffer.indexOf(curr);
    			   
    			   if(linearizability.seqBuffer.get(currIndex).currGlobalSeq == (linearizability.localSequence+1) )
    			   {
    				   System.out.println("found message in SeqBuffer");
    			   }
    				   
    		   }
    		   else
    		   {
    			   linearizability.recMessages.add(curr);
    		   }
    	   }

       }
       else
       {
    	   char[] cmd = clientMessage[0].toCharArray();
           char cmdType = cmd[0];
       	   int clientId = -1;
	       clientId = clientAdder(server, Integer.parseInt(clientMessage[1]));
	       
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
       }
       
       server.close();
       
   }

   //Collects client information and adds it to the client list
   private int clientAdder(Socket server, int cliPort) throws UnknownHostException, IOException 
   {
	   String clientSockAdd = server.getRemoteSocketAddress().toString();
	   
       String[] clientInfo = clientSockAdd.split(":");
       StringBuilder sb = new StringBuilder(clientInfo[0]);
       sb.deleteCharAt(0);
       
       InetAddress clientIP = InetAddress.getByName(sb.toString());
       int clientPort = cliPort;
       ProcessInfo client = new ProcessInfo(clientIP, clientPort);
       
       int clientId = clientIdentifier(clientPort);
       if(clientId == -1)
       {
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
		   
		   //multicast 
		   
		  String msg = new String(cmd);
		  broadcaster(msg);
		  
		   //responds to the client
		   Responder responder = new Responder(logLineResp, min_delay, max_delay, clients.get(clientId), outputFileName, 1);
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
		   
		   String msg = new String(cmd);
		   broadcaster(msg);
		   
		   int value = findGetValue(variable);
		   String logLineResp = "";
		   
		   if(value == -1)
		   {
			   logLineResp = logLine.concat(",resp,NA\n");
		   }
		   else
		   {
			   logLineResp = logLine.concat(",resp,"+value+"\n");
		   }
		   
		   Responder responder = new Responder(logLineResp, min_delay, max_delay, clients.get(clientId), outputFileName, 2);
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
	   
	   Responder responder = new Responder("", min_delay, max_delay, clients.get(clientId), outputFileName,3);
	   responder.start();
   }
   
   private void broadcaster(String msg) throws IOException 
   {
		  
	   for(Integer i : replicas.keySet())
	   {
		   try
		   {
			   Socket sendSock = new Socket();
			   ProcessInfo toSend = replicas.get(i);
			   sendSock.connect(new InetSocketAddress(toSend.getIP(), toSend.getPort())); 
		       DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
		       out.writeUTF("TO "+"M "+msg+" "+ Integer.toString(this.id)+" "+ Integer.toString(-1));
		       sendSock.close();
			}
			catch(SocketException s)
			{
				continue;
			}
	   	}
	
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
		
		for(Integer i : clients.keySet())
		{
			if(clients.get(i).getPort() == sourcePort)
			{
				src = i;
				break;
			}
		}
		
		return src;
  }
   

   
   public void run()
   {
      while(true)
      {
         try
         {	 
        	
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
       
      }
      catch(IOException e)
      {
         e.printStackTrace();
      }
   }
}
