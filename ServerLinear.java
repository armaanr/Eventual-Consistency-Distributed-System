import java.net.*;
import java.util.*;
import java.io.*;

public class ServerLinear extends Thread
{
   //accepts connections
   public ServerSocket serverSocket;
   
   //contains the information about all processes
   public int id;
   public Map<Integer, ProcessInfo> replicas;
   public Map<Integer, ProcessInfo> clients;
   public Map<String, Integer> data;
   public int maxClient;
   public String outputFileName;
   public int min_delay;
   public int max_delay;
   
   //for linearizability
   public TotalOrderMulticast linearizability;
   public boolean sequencer;
   public int globalSequence;
   
   public ServerLinear(int port) throws IOException
   {
      this.serverSocket = new ServerSocket(port);
      this.replicas = new HashMap<Integer, ProcessInfo>();
      this.clients = new HashMap<Integer, ProcessInfo>();
      this.data = new HashMap<String, Integer>();
      this.outputFileName = "../outputs/output_log";
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
    	   totalOrderHandler(clientMessage);

       }
       else
       {
    	   clientHandler(server, clientMessage);
       }
       
       server.close();
       
   }

   //handles direct client messages
	private void clientHandler(Socket server, String[] clientMessage) throws UnknownHostException, IOException, FileNotFoundException 
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

	//handles 'TO' messages required for implementing Total Order Multicast
	private void totalOrderHandler(String[] clientMessage) throws IOException 
	{
		System.out.println("checking TO messages");
		   
		   //parses the message
		   String currMsg = clientMessage[2];
		   int origId = Integer.parseInt(clientMessage[3]);
		   int currgs = Integer.parseInt(clientMessage[4]);
		   int clientId = Integer.parseInt(clientMessage[5]);
		   String logLine = clientMessage[6];
		   TotalOrderMulticast.TotalOrderInfo curr = linearizability.new TotalOrderInfo(currMsg, currgs, origId);
		   
		   //handles broadcast messages
		   if(clientMessage[1].equals("M"))
		   {
			   int seqIndex = linearizability.sequencerCheck(currMsg, origId, linearizability.localSequence);
			   
			   if(seqIndex != -1)
			   {
				   linearizability.localSequence++;
				   TotalOrderMulticast.TotalOrderInfo update = linearizability.seqBuffer.remove(seqIndex);
				   System.out.println("message=> "+update.message+" Id => "+update.id+" global=> "+update.currGlobalSeq );
				   System.out.println("seqBuffer size =>" + linearizability.seqBuffer.size()+"\n");
				   respond(clientId, logLine, update, origId);
			   }
			   else
			   {
				   System.out.println("added to recMessages\n");
				   linearizability.recMessages.add(curr);
			   }
			   
			   if(sequencer)
			   {
				   globalSequence++;
				   broadcaster(curr.message, true, clientId, logLine, origId);
			   }
		   }
		   
		   //handles sequencer messages
		   if(clientMessage[1].equals("S"))
		   {
			   int recIndex = linearizability.messagesCheck(currMsg, origId);
			  
			   if(recIndex != -1)
			   {
				   TotalOrderMulticast.TotalOrderInfo update = linearizability.recMessages.remove(recIndex); 
				   System.out.println("message=> "+update.message+" Id => "+update.id+" global=> "+update.currGlobalSeq );
				   System.out.println("recMessages size =>" + linearizability.recMessages.size()+"\n");
				   
				   respond(clientId, logLine, update, origId);
				   
			   }
			   else
			   {
				   System.out.println("added to seqBuffer");
				   System.out.println("recMessages size =>" + linearizability.recMessages.size()+"\n");
				   linearizability.seqBuffer.add(curr);
			   }
		   }
	}

   //Updates data and responds to the client(if client is connected to this server)
   private void respond(int clientId, String logLine, TotalOrderMulticast.TotalOrderInfo update, int origServer) 
   {
	   String outputWrite = "";
	   char[] cmd = update.message.toCharArray();
	   char cmdType = cmd[0];
	   String variable = Character.toString(cmd[1]);
	   
	   if(cmdType == 'p')
	   {
		   String value = Character.toString(cmd[2]);
		   data.put(variable, Integer.parseInt(value));
		   
		   if(origServer == this.id) 
		   {
			   outputWrite = logLine.concat(",resp,"+value+"\n");
			   System.out.println("outputWrite =>" + outputWrite);
			   
			   ResponderLinear responderLinear = new ResponderLinear(outputWrite, clients.get(clientId), outputFileName, 1);
			   responderLinear.start();
		   }
	   }
	   else if(cmdType == 'g')
	   {
		   if(origServer == this.id)
		   {
			   if(data.containsKey(variable))
			   {
				   int retVal = data.get(variable);
				   
				   if(retVal == -1)
				   {
					   outputWrite = logLine.concat(",resp,-1\n");
				   }
				   else
				   {
					   outputWrite = logLine.concat(",resp,"+Integer.toString(retVal)+"\n");
				   } 
			   }
			   System.out.println("outputWrite =>" + outputWrite);
			   
			   
			   ResponderLinear responderLinear = new ResponderLinear(outputWrite, clients.get(clientId), outputFileName, 2);
			   responderLinear.start();
		   }
		   
	   }
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
   
   //logs the put request and broadcasts the commands
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
		   
		   Writer output = new BufferedWriter(new FileWriter(outputFileName, true));
		   output.append(logLineReq);
		   output.close();
		   
		   //multicast 
		   String msg = new String(cmd);
		   broadcaster(msg,false,clientId,logLine,-1);
		   
	   } 
	   catch (IOException e) 
	   {
		e.printStackTrace();
	   }
	   
   }
   
   //logs the get request and broadcasts the message
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
		   broadcaster(msg, false, clientId, logLine,-1);
		   
	   } 
	   catch (IOException e) 
	   {
		e.printStackTrace();
	   }
   }
   
   //handles the dump command
   private void dumpHandler(char[] cmd ,int clientId) throws FileNotFoundException
   {
	   @SuppressWarnings("resource")
	   Scanner scanner = new Scanner(new File(outputFileName));
	   
	   while(scanner.hasNextLine())
	   {
		   System.out.println(scanner.nextLine());
	   }
	   
	   ResponderLinear responderLinear = new ResponderLinear("", clients.get(clientId), outputFileName,3);
	   responderLinear.start();
   }
   
   private void broadcaster(String msg, boolean seq, int clientId, String logLine, int origServerId) throws IOException 
   {
		  
	   for(Integer i : replicas.keySet())
	   {

		   ProcessInfo toSend = replicas.get(i);
		   String sendMessage = "";
		  
	       if(seq)
	       {
	    	   sendMessage = "TO "+"S "+msg+" "+ Integer.toString(origServerId)+" "+ Integer.toString(globalSequence)+" "+clientId+" "+logLine;
	       }
	       else
	       {
	    	   sendMessage = "TO "+"M "+msg+" "+ Integer.toString(this.id)+" "+ Integer.toString(-1)+" "+clientId+" "+logLine;
	       }
	       
	       BroadcasterLinear broadcast = new BroadcasterLinear(min_delay, max_delay, toSend, sendMessage);
	       broadcast.start();
		       
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
    	 ServerLinear reciever = new ServerLinear(port);
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
