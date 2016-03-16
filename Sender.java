import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;

public class Sender extends Thread {
	
	public int destination;
	public int port;
	public String message;
	public int min_delay;
	public int max_delay;
	public ProcessInfo destInfo;
	
	public Sender(int dest, ProcessInfo dI, String msg, int min, int max, int prt)
	{
		this.destination = dest;
		this.message = msg;
		this.min_delay = min;
		this.max_delay = max;
		this.destInfo = dI;
		this.port = prt;
	}
	
	//Opens a socket connection and sends a message to another process
	public void unicast_send() throws IOException
	{
		
		InetAddress destIp = this.destInfo.getIP();
		int destPort = this.destInfo.getPort();
		   
		Socket sendSock = new Socket(destIp, destPort);	   
		DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
		
		//adding port to string; helps receiving server identify the process
		String smessage = this.message.concat(" " + port);		
		System.out.println("Sent \"" + this.message + "\"  to process " + this.destination + ", system time is ­­­­­­­­­­­­­" + System.currentTimeMillis());
		
		out.writeUTF(smessage);
		
		sendSock.close();
		  
		   
	  }
	
	//generates delays based on min/max delay
	private void delayGenerator() {
		if(this.min_delay > 0 && this.max_delay > 0)
		{
			if(this.max_delay >= this.min_delay )
			{
				Random r = new Random();
				int randomDelay = r.nextInt(this.max_delay - this.min_delay) + this.min_delay;
				try {
					Thread.sleep(randomDelay);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			System.out.println("max is smaller than min");
			
		}
	}
	  
	public void run()
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
	        	 
	        	 
		 	try {
		 			delayGenerator();
		 			unicast_send();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        
	         
	         
	      
	   }

	

}
