
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Random;

public class BroadcasterLinear extends Thread {
	
	public int min_delay;
	public int max_delay;
	public ProcessInfo server;
	public String message;
	
	public BroadcasterLinear(int min, int max, ProcessInfo serv, String msg)
	{
		this.min_delay = min;
		this.max_delay = max;
		this.server = serv;
		this.message = msg;
	}

	//generates delays based on min/max delay
	private void delayGenerator() 
	{
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
			 try 
			 {
				 throw new InterruptedException();
			 } 
			 catch (InterruptedException e) 
			 {
				 e.printStackTrace();
			 }
		  } 
		 	
	 	try 
	 	{
	 		delayGenerator();
	 		
	 		Socket sendSock = new Socket();
	 		ProcessInfo toSend = server;
	 		
	 		try
	 		{
	 			sendSock.connect(new InetSocketAddress(toSend.getIP(), toSend.getPort())); 
	 		}
	 		catch(ConnectException c)
	 		{
	 			System.out.println("connecting");
	 		}
	 		
	 		try
	 		{
		 		DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
		 		out.writeUTF(message);
		    	out.close();
	 		}
	 		catch(SocketException s)
	 		{
	 			System.out.println("connecting");
	 		}
	    	
	 		
	        sendSock.close();
	 		
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	 	
	 	
	 	
	      
	}
	
	
}
