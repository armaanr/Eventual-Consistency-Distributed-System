import java.net.*;
import java.util.*;
import java.io.*;

public class Broadcaster extends Thread
{
    String msg;
    ProcessInfo dest;
    int min_delay;
    int max_delay;

    public Broadcaster(ProcessInfo dest, String msg, int min_delay, int max_delay)
    {
        this.dest = dest;
        this.msg = msg;
        this.min_delay = min_delay;
        this.max_delay = max_delay;
    }

    private void send_msg() throws IOException
    {
        delayGenerator();
        Socket sendSock = new Socket();
        sendSock.connect(new InetSocketAddress(dest.getIP(), dest.getPort())); 
        DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
        out.writeUTF(this.msg);
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
					e.printStackTrace();
				}
			}
			else
                System.out.println("max is smaller than min");
		}
	}

   public void run()
   {
         try
         {	 
            send_msg();
         }
         catch(SocketTimeoutException s)
         {
            System.out.println("Socket timed out!");
         }
         catch(IOException e)
         {
            this.dest.alive = false;
         }
   }
}
