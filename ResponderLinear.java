import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;

public class ResponderLinear extends Thread {
	
	public String logLineResp;
	public String outputFileName;
	public ProcessInfo clientInfo;
	public int cmdType;
	
	public ResponderLinear(String msg, ProcessInfo info, String outFile, int ctype)
	{
		this.logLineResp = msg;
		this.clientInfo = info;
		this.outputFileName = outFile;
		this.cmdType = ctype;
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
	
	public void getResponse() throws IOException
	{
		
		Socket sendSock = new Socket(clientInfo.getIP(), clientInfo.getPort());	   
		DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
		String[] message = logLineResp.split(",");
		out.writeUTF(message[6]);
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
		 	
		 	try {
		 		//delayGenerator();
		 		
		 		if(cmdType == 1)
		 		{
		 			putResponse();
		 		}
		 		else if(cmdType == 2)
		 		{
		 			getResponse();
		 		}
		 		else if(cmdType == 3)
		 		{
		 			dumpResponse();
		 		}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	         
	      
	   }

	

}
