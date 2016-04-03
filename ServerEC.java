import java.net.*;
import java.util.*;
import java.io.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ServerEC extends Thread
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
   public int message_count;
   public Lock mutex;
   public Lock mutex2;
   public int rval;
   public int wval;

   // Stores local variable values.
   public Map<String, EventualInfo> variables;

   // Stores number of required messages.
   public Map<Integer, MessageInfo> message_table;
   
   public ServerEC(int port, int rval, int wval) throws IOException
   {
      this.serverSocket = new ServerSocket(port,10);
      this.replicas = new HashMap<Integer, ProcessInfo>();
      this.clients = new HashMap<Integer, ProcessInfo>();
      this.variables = new HashMap<String, EventualInfo>();
      this.message_table = new HashMap<Integer, MessageInfo>();
      this.outputFileName = "outputs/output_log";
      this.maxClient = 0;
      this.min_delay= 0;
      this.max_delay = 0;
      this.message_count = 0;
      this.mutex = new ReentrantLock();
      this.mutex2 = new ReentrantLock();
      this.rval = rval;
      this.wval = wval;
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
            if (id == this.id)
                procInfo.alive = false;
		    
		    replicas.put(id, procInfo);    
	    }
	   
	    //sets the name of the output file
	    this.id = processIdentifier(this.serverSocket.getLocalPort());
	    String processName = Integer.toString(this.id).concat(".txt"); 
	   	this.outputFileName = this.outputFileName.concat(processName);
   }

   //parses received messages and accordingly allocates tasks
   public void receiver() throws IOException
   {
       while (true)
        {
	   System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
       Socket server = this.serverSocket.accept();
       
       //receives message
       DataInputStream in = new DataInputStream(server.getInputStream());
       String received = "";
       received = in.readUTF();  
       
       System.out.println("message rcvd => ("+ received+")");
       
       String[] message = received.split(" ");
       
       /*
        * If a replica sent the message, then we only need to
        * respond to the replica and update our value if necessary.
        */
       if(message[0].equals("EC"))
       {
               String type = message[1];

               if (message[1].equals("M"))
               {
                   String msg = message[2];
                   int return_id = Integer.parseInt(message[6]);
                   String var = "" + msg.charAt(1);
                   EventualInfo var_info;
                   this.mutex.lock();
                   if (!this.variables.containsKey(var))
                   {
                        var_info = new EventualInfo(var, -1, 0);
                        this.variables.put(var, var_info);
                   }
                   else
                        var_info = this.variables.get(var); 
                   this.mutex.unlock();
                   EventualResponder responder = 
                              new EventualResponder(message,
                                                    this.min_delay,
                                                    this.max_delay,
                                                    this.replicas.get(return_id),
                                                    var_info,
                                                    this.mutex,
                                                    this.id);
                   responder.start();
               }
               else if (message[1].equals("A"))
               {
                   ack_handler(message);
               }
               else
                   System.out.println("Invalid message format: " + received);
       }
       /* 
        * Otherwise, we must multicast the message to all other
        * active replicas.
        */
       else
       {
    	   char[] cmd = message[0].toCharArray();
           char cmdType = cmd[0];
	       int clientId = clientAdder(server, Integer.parseInt(message[1]));
	       
	       //handles 'send' commands from command line
	       if(cmdType == 'p')
	       {
               this.message_count++;
	    	   putHandler(cmd, clientId, this.message_count);   
	       }
	       else if(cmdType == 'g')
	       {
               this.message_count++;
	    	   getHandler(cmd, clientId, this.message_count); 
	       }
	       else if(cmdType == 'd')
	       {
	    	   dumpHandler(cmd, clientId); 
	       }
       }
       
       server.close();
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
   
   //Handles the 'put' command from the client and updates the output file.
   private void putHandler(char[] cmd,int clientId, int m_id)
   {
	   try 
	   { 
		   //prepare put log info
		   String logLine = "666,";
		   String intId = Integer.toString(clientId);
		   String variable = Character.toString(cmd[1]);
		   String value = Character.toString(cmd[2]);
		   
           // Change this replica's value as specified by client.
           EventualInfo var_info;
           this.mutex.lock();
           if (!this.variables.containsKey(variable))
           {
                var_info = new EventualInfo(variable, Integer.parseInt(value), 0);
                this.variables.put(variable, var_info);
           }
           else
                var_info = this.variables.get(variable); 
           int new_timestamp = var_info.timestamp + 1;
           var_info.update(Integer.parseInt(value), new_timestamp, this.id, this.id);
           this.mutex.unlock();

           // Sets up the message table so we know how many responses to wait for.
           MessageInfo msg_info = new MessageInfo(this.wval, clientId, cmd, Integer.parseInt(value),new_timestamp, variable);
           this.mutex2.lock();
           this.message_table.put(m_id, msg_info);
           this.mutex2.unlock();
		   
		   // Log the request.
		   logLine = logLine.concat(intId+",put,"+variable+","+System.currentTimeMillis());
		   String logLineReq = logLine.concat(",req,"+value+"\n");
		   
		   Writer output = new BufferedWriter(new FileWriter(outputFileName, true));
		   output.append(logLineReq);
		   output.close();

		   // Multicast message to all other replicas. 
		   String msg = "EC "
                        +"M "
                        +cmd[0] +cmd[1] +cmd[2]+ " "
                        +value + " "
                        +Integer.toString(new_timestamp) + " "
                        +Integer.toString(m_id) + " "
                        +Integer.toString(this.id);
		   broadcaster(msg);
	   } 
	   catch (IOException e) 
	   {
		e.printStackTrace();
	   }
   }

    private void ack_handler(String[] msg)
    {
        int m_id = Integer.parseInt(msg[2]);
        int cmdType;
        String logLine = "666,";

        this.mutex2.lock();
        MessageInfo msg_info = this.message_table.get(m_id);
        int remaining = msg_info.remaining - 1;
        msg_info.remaining--;
        int intId = msg_info.client_id;
        String variable = msg_info.variable;
        String value = Integer.toString(msg_info.value);
        int timestamp = msg_info.timestamp;
        this.mutex2.unlock();

        if (remaining == 0)
        {
            // This means it is a put request.
            if (msg.length == 3)
            {
                logLine = logLine.concat(intId+",put,"+variable+","+ System.currentTimeMillis());
                cmdType = 1;
            }
            // Otherwise it is a get request.
            else
            {
                logLine = logLine.concat(intId+",get,"+variable+","+ System.currentTimeMillis());
                cmdType = 2;
            }
            logLine = logLine.concat(",resp,"+value+"\n");
            Responder responder = new Responder(logLine, min_delay, max_delay, clients.get(intId), outputFileName, cmdType);
            responder.start();
        }
        // TODO: fix this to be the number of ALIVE replicas
        // not all of them.
        else if (remaining <= this.wval - this.replicas.size())
        {
            this.mutex2.lock();
            this.message_table.remove(m_id);
            this.mutex2.unlock();
        }
        // Choose the value with the largest timestamp to return to the client.
        else if (remaining > 0 && msg.length == 5)
        {
            int their_value = Integer.parseInt(msg[3]);
            int their_timestamp = Integer.parseInt(msg[4]);
            this.mutex2.lock();
            MessageInfo my_info = this.message_table.get(m_id);
            //TODO implement tie breaking
            if (my_info.timestamp < their_timestamp)
            {
                my_info.value = their_value;
                my_info.timestamp = their_timestamp;
            }
            this.mutex2.unlock();
        }
    }
   
   private void getHandler(char[] cmd, int clientId, int m_id)
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

           // Sets up the message table so we know how many responses to wait for.
           this.mutex.lock();
           EventualInfo my_info = this.variables.get(variable);
           int my_value = my_info.value;
           int my_timestamp = my_info.timestamp;
           this.mutex.unlock();
           this.mutex2.lock();
           MessageInfo msg_info = new MessageInfo(this.rval, clientId, cmd, my_value, my_timestamp, variable);
           this.message_table.put(m_id, msg_info);
           this.mutex2.unlock();
		   
		   // Multicast message to all other replicas. 
           // The -1 fields don't matter in this context.
		   String msg = "EC "
                        +"M "
                        +cmd[0] +cmd[1] + " "
                        +"-1" + " "
                        +"-1" + " "
                        +Integer.toString(m_id) + " "
                        +Integer.toString(this.id);
		   broadcaster(msg);
		   
	   } 
	   catch (IOException e) 
	   {
		e.printStackTrace();
	   }
    }

    private void dumpHandler(char[] cmd ,int clientId) throws FileNotFoundException
    {
        // Prints values of all variables to stdout.
        for (String key : this.variables.keySet())
        {
            System.out.println(key + " = " + Integer.toString(this.variables.get(key).value));
        }

        Responder responder =
            new Responder("", min_delay, max_delay, clients.get(clientId), outputFileName,3);
        responder.start();
    }
   
   private void broadcaster(String msg) 
   {
       ProcessInfo toSend = null;
	   for(Integer i : replicas.keySet())
	   {
			   toSend = replicas.get(i);
               if (toSend.alive && i != this.id) {
                    Broadcaster sender = new Broadcaster(toSend,
                                                         msg,
                                                         this.min_delay,
                                                         this.max_delay);
                    sender.start();
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
         try
         {	 
            receiver();
         }
         catch(SocketTimeoutException s)
         {
            System.out.println("Socket timed out!");
         }
         catch(IOException e)
         {
            e.printStackTrace();
         }
   }
   
  public static void main(String [] args) throws IOException
  { 
      try
      {
    	 int port = Integer.parseInt(args[0]);
         int rval = Integer.parseInt(args[2]);
         int wval = Integer.parseInt(args[3]);
         
         // Starts the server.
    	 ServerEC reciever = new ServerEC(port, rval, wval);
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
