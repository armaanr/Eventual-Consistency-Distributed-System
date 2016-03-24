import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;

public class Responder extends Thread {
	
	public String logLineResp;
	public String outputFileName;
	public int min_delay;
	public int max_delay;
	public ProcessInfo clientInfo;
	
	public Responder(String msg, int min, int max, ProcessInfo info, String outFile)
	{
		this.logLineResp = msg;
		this.min_delay = min;
		this.max_delay = max;
		this.clientInfo = info;
		this.outputFileName = outFile;
	}
	
	//Opens a socket connection and sends a message to another process
	public void putResponse() throws IOException
	{
		   
		Socket sendSock = new Socket(clientInfo.getIP(), clientInfo.getPort());	   
		DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
		out.writeUTF("A");
		out.close();
		
		Writer output = new BufferedWriter(new FileWriter(outputFileName, true));
		output.append(logLineResp);
		output.close();
		
		sendSock.close();
		  
		   
	 }
	
	//
	public void getResponse() throws IOException
	{
		
		Socket sendSock = new Socket(clientInfo.getIP(), clientInfo.getPort());	   
		DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
		String[] message = logLineResp.split(",");
		out.write(Integer.parseInt(message[6]));
		out.close();
		
		Writer output = new BufferedWriter(new FileWriter(outputFileName, true));
		output.append(logLineResp);
		output.close();
		
		sendSock.close();
		  
		   
	  }
	
	public void dumpResponse() throws IOException
	{
		Socket sendSock = new Socket(clientInfo.getIP(), clientInfo.getPort());	   
		DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
		out.writeUTF("A");
		out.close();
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
				putResponse();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
	         
	         
	      
	   }

	

}
