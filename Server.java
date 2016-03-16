import java.net.*;
import java.util.*;
import java.io.*;

public class Server extends Thread
{
   //accepts connections
   public ServerSocket serverSocket;
   
   //contains the information about all processes
   public Map<Integer, ProcessInfo> otherProcesses;
   public int min_delay;
   public int max_delay;
   
   //check if Total(1) or Causal(2)
   public int multCheck;
   
   //for Causal Ordering
   CausalOrderMulticaster causalOrder;
   
   //for total ordering
   public static int globalSequence;
   public boolean multicastLeader;
   public TotalOrderMulticaster totalOrder;
   
   public Server(int port) throws IOException
   {
      serverSocket = new ServerSocket(port);
      otherProcesses = new HashMap<Integer, ProcessInfo>();
      serverSocket.setSoTimeout(150000);
      globalSequence = 1;
      
      this.multCheck = -1;
      this.multicastLeader = false;
      this.totalOrder = new TotalOrderMulticaster();
      
      causalOrder = new CausalOrderMulticaster();
   }
   
   //Parses the given config file to initialize the Server variables
   public void readConfig(File file) throws IOException
   {
	   @SuppressWarnings("resource")
	   Scanner scanner = new Scanner(file);
	   
	   if(scanner.hasNext())
	   {
		   String[] delays = scanner.nextLine().split(" ");
		   this.min_delay = Integer.parseInt(delays[0]);
		   this.max_delay = Integer.parseInt(delays[1]);
	   }

	   while(scanner.hasNext())
	   {
		    String[] tokens = scanner.nextLine().split(" ");
		    
		    int id = Integer.parseInt(tokens[0]);
		    InetAddress ip = InetAddress.getByName(tokens[1]);
	        int port = Integer.parseInt(tokens[2]); 
		    
		    ProcessInfo procInfo = new ProcessInfo(ip, port);
		    
		    otherProcesses.put(id, procInfo);
		    
	    }
	   
	   if(otherProcesses.get(1).getPort() == this.serverSocket.getLocalPort())
	   {
		   this.multicastLeader = true;
	   }
	   
	   
   }
   
   

   //parses received messages and accordingly allocates tasks
   public void receiver() throws IOException
   {
	   
	   System.out.println("Waiting for client on port " + serverSocket.getLocalPort() + "...");
       Socket server = this.serverSocket.accept();
       
       
       //receives message
       DataInputStream in = new DataInputStream(server.getInputStream());
       String message = "";
       message = in.readUTF();   
       String[] cmd = message.split(" ");
       
       //handles 'send' commands from command line
       if(cmd[cmd.length - 1].equals("cmd"))
       {
    	   unicastHandler(cmd);   
    	  
       }
       //handles 'msend' commands from command line
       else if(cmd[cmd.length - 1].equals("mcmd"))
       {
    	   multicastHandler(cmd);
       }
       //handles proxy messages sent to leader from other processes(TOTAL ORDERING)
       else if(cmd[cmd.length - 2].equals("msendproxy"))
       {
    	   int sendPort = Integer.parseInt(cmd[cmd.length - 1]);
    	   leaderSendHandler(cmd, sendPort);
       }
       //handles messages if sent from a leader(TOTAL ORDERING)
       else if(cmd[cmd.length - 2].equals("leader"))
       {
    	   globalSequence++;
    	   multicast_recieve(cmd);
       }
       //handles messages sent with vector clock(CAUSAL ORDERING)
       else if(cmd[cmd.length - 2].equals("causal"))
       {   
    	   causalOrderHandler(cmd);   	   
       }
       else
       {
    	   unicast_recieve(server, message);
       }
       
       
       server.close();
       multicastBufferChecks();
       
   }

//Runs loops on the multicast buffers to check if they are eligible for delivery
private void multicastBufferChecks() {
	
		//Checks according to Total Ordering rules
	   if(multCheck == 1)
	   {
		   Iterator<TotalOrderInfo> iter = totalOrder.buffer.iterator();
		   while(iter.hasNext())
		   {
			   TotalOrderInfo curr = iter.next();
			   String toDeliver = curr.message;
			   int src = processIdentifier(curr.port);
			   
			   if(curr.globalSequence == (this.totalOrder.localsequence + 1) )
			   {
				   this.totalOrder.localsequence++;
				   System.out.println("Received \"" + toDeliver.trim() + "\"  from process " + src + ", system time is ­­­­­­­­­­­­­" + System.currentTimeMillis());
			   }
		   }
	   }
	   //Checks according to Causal Ordering Rules
       else if(multCheck == 2)
       {
    	   Iterator<CausalOrderInfo> iter = causalOrder.buffer.iterator();
    	   
    	   while(iter.hasNext())
    	   {
    		   CausalOrderInfo b = iter.next();
    		   int mClock[] = causalOrder.strToArr(b.mClock);
    		   int src = b.port;
    		   int check = causalOrderCheck(mClock, src-1);
    		   
    		   if(check == 4)
    		   {
    			   causalOrder.clock[src-1] = mClock[src-1] ;
    			   System.out.println("Received \"" + b.message.trim() + "\"  from process " + src + ", system time is ­­­­­­­­­­­­­" + System.currentTimeMillis());
    			   iter.remove();
    		   }
    		  
    		   
    	   }
       }
}

//Delivers messages sent from the same port and buffers all messages from other sources
private void causalOrderHandler(String[] cmd) {
	   
	   int src = processIdentifier(Integer.parseInt(cmd[cmd.length - 1]));
	   int srcIndex = src - 1;
	   
	   String recMessage = "";	  
	   for( int i = 0; i< cmd.length-3; i++)
	   {
		   recMessage = recMessage.concat(cmd[i] + " ");
	   }
	   
	   //if command comes from the same port, then it increments clock and recieves otherwise it buffers
	   if(serverSocket.getLocalPort() == src)
	   {
		   causalOrder.clock[srcIndex]++;
		   System.out.println("Received \"" + recMessage.trim() + "\"  from process " + src + ", system time is ­­­­­­­­­­­­­" + System.currentTimeMillis());
	   }
	   else
	   {
		   causalOrder.buffer.add(new CausalOrderInfo(recMessage,cmd[cmd.length - 3],src));
	   }
}

//Checks whether the message clock and the vector clock satisfy causality conditions
private int causalOrderCheck(int[] mClock, int srcIndex) {
		
		int check = 0;
		
	   if(mClock[srcIndex] == (causalOrder.clock[srcIndex] + 1))
		   check++;
	   
	   for(int i = 0; i< mClock.length; i++)
	   {
		   
		   if( mClock[i] <= causalOrder.clock[i])
		   {
			   check++;
		   }
			   
	   }
	
	   return check;
}

//adds messages to total order buffer (TOTAL ORDERING)
private void multicast_recieve(String[] cmd) {
	   String sentMessage = "";
	   for( int i = 0; i< cmd.length-2; i++)
	   {
		   sentMessage = sentMessage.concat(cmd[i] + " ");
	   }
	   TotalOrderInfo curr = new TotalOrderInfo(sentMessage , globalSequence, Integer.parseInt(cmd[cmd.length - 1]));
	   totalOrder.buffer.add(curr);
}  

   
//handles msend commands based on the type of multicast
private void multicastHandler(String[] cmd) 
   {
	   // Total Ordering
	   if(this.multCheck == 1)
	   {
		   if(multicastLeader)
		   {
			   leaderSendHandler(cmd, serverSocket.getLocalPort());
		   }
		   else
		   {
			   //sends message to leader who then broadcasts it to everyone
			   ProcessInfo destInfo = otherProcesses.get(1);
			   String sendMessage = "";
			  
			   for( int i = 0; i< cmd.length-1; i++)
			   {
				   sendMessage = sendMessage.concat(cmd[i] + " ");
			   }
			   
			   sendMessage = sendMessage.concat("msendproxy");
			   
			   Sender send = new Sender(1 , destInfo, sendMessage.trim(), min_delay, max_delay, serverSocket.getLocalPort());
			   send.start();
		   }
	   }
	   // Causal Ordering
	   else if(this.multCheck == 2)
	   {   
		   int currId = processIdentifier(serverSocket.getLocalPort());
		   causalOrder.clock[currId-1]++;
		   
		   for(int id = 0; id< otherProcesses.size(); id++)
		   {
			   ProcessInfo destInfo = otherProcesses.get(id+1);
			   String sendMessage = "";
			   
			   for( int i = 1; i< cmd.length-1; i++)
			   {	   
				   sendMessage = sendMessage.concat(cmd[i] + " ");
			   }
			   sendMessage = sendMessage.concat(causalOrder.toString() + " causal ");
			   
			   Sender send = new Sender(id+1, destInfo, sendMessage.trim(), min_delay, max_delay, serverSocket.getLocalPort());
			   send.start();
			    
		   }
	   }
	     	
   }

//Makes the leader thread send a multicast to all processes(TOTAL ORDERING)
private void leaderSendHandler(String[] cmd, int sendPort) {
	   
		
	   for(int id = 0; id< otherProcesses.size(); id++)
	   {
		   ProcessInfo destInfo = otherProcesses.get(id+1);
		   
		   String sendMessage = "";
		   for( int i = 1; i< cmd.length-1; i++)
		   {
			   if(cmd[i].equals("msendproxy"))
				continue;   
			   sendMessage = sendMessage.concat(cmd[i] + " ");
		   }
		   sendMessage = sendMessage.concat("leader");
		   
		   Sender send = new Sender(id+1, destInfo, sendMessage.trim(), min_delay, max_delay, sendPort);
		   send.start();
		    
	   }
}
   

   //spawns a sender thread for received 'send' commands
   private void unicastHandler(String[] cmd) 
   {
	
	   int dest = Integer.parseInt(cmd[1]);
	   ProcessInfo destInfo = null;
	   
	   if(otherProcesses.containsKey(dest))
	   {
		   destInfo = otherProcesses.get(dest);
		   String sendMessage = "";
		   for(int i = 2; i< cmd.length - 1; i++)
		   {

			   sendMessage = sendMessage.concat(cmd[i] + " ");

		   }
		   
		   Sender sender = new Sender(dest, destInfo, sendMessage.trim(), min_delay, max_delay, serverSocket.getLocalPort());
		   sender.start();
	   }
	   else
	   {
		   System.out.print("Destination process does not exist on network");
	   }
   }

   //delivers received messages
   private void unicast_recieve(Socket server, String message) 
   {
	    String[] cmd = message.split(" ");
	    int sourcePort = Integer.parseInt(cmd[cmd.length - 1]);
	    int src = processIdentifier(sourcePort);
	    
	    String recMessage = "";
 	   	for(int i = 0; i< cmd.length - 1; i++)
 	   	{

 		   recMessage = recMessage.concat(cmd[i] + " ");

 	   	}
	   	
	   	System.out.println("Received \"" + recMessage.trim() + "\"  from process " + src + ", system time is ­­­­­­­­­­­­­" + System.currentTimeMillis());
   }

  //returns the id of a process using it's port number
  private int processIdentifier(int sourcePort) {
	int src = 0;
	for(Integer i : otherProcesses.keySet())
	{
		if(otherProcesses.get(i).getPort() == sourcePort)
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
         String fileName = args[2];
         File file = new File(fileName);
         reciever.readConfig(file);
         
         //set multicast protocol
    	 reciever.multCheck = Integer.parseInt(args[1]);  
         
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