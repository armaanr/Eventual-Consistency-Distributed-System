import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
 * Handles sending ACKS to replicas for put and get requests.
 * ACKS to clients are implement in Responder.java
 * Broadcasts are implemented in ServerEC.java
 */
public class EventualResponder extends Thread
{
    public String[] msg_info;
    public int min_delay;
    public int max_delay;
    public ProcessInfo p_info;
    public EventualInfo var_info;
    public Lock mutex;

    // To break ties.
    public int my_id;
    
    public EventualResponder(String[] msg_info,
                             int min_delay,
                             int max_delay,
                             ProcessInfo p_info,
                             EventualInfo var_info,
                             Lock mutex,
                             int my_id)
    {
        this.msg_info = msg_info;
        this.min_delay = min_delay;
        this.max_delay = max_delay;
        this.p_info = p_info;
        this.var_info = var_info;
        this.mutex = mutex;
        this.my_id = my_id;
    }

    /*
     * Updates the value and timestamp as necessary and then sends
     * an ACK.
     */
	public void putResponse() throws IOException
	{
        this.mutex.lock();
        int new_value = Integer.parseInt(msg_info[3]);
        int new_timestamp = Integer.parseInt(msg_info[4]);
        int other_id = Integer.parseInt(msg_info[6]);
        this.var_info.update(new_value, new_timestamp, this.my_id, other_id);
        this.mutex.unlock();

        delayGenerator();
		Socket sendSock = new Socket(p_info.getIP(), p_info.getPort());	   
		DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());

        out.writeUTF("EC " + "A " + msg_info[5]);

		out.close();
		sendSock.close();
	 }
	
    /*
     * Sends an ACK containing the value of the variable, timestamp, and
     * message_id. The updating of values and timestamps is taken care of
     * on the receiving side for this type of request. 
     */
	public void getResponse() throws IOException
	{
        // In case the value or timestamp changes between now and after
        // the delay.
        this.mutex.lock();
        String value_snapshot = Integer.toString(this.var_info.value);
        String timestamp_snapshot = Integer.toString(this.var_info.timestamp);
        this.mutex.unlock();

		Socket sendSock = new Socket(p_info.getIP(), p_info.getPort());	   
		DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());

        delayGenerator();
        out.writeUTF("EC "
                     +"A "
                     +this.msg_info[5] + " "
                     +value_snapshot + " "
                     +timestamp_snapshot);
		
		out.close();
		sendSock.close();
	  }
	
    /*
     * Decides on a response based on message format.
     */
    public void choose_response() throws IOException
    {
            if (this.msg_info[2].charAt(0) == 'p')
                this.putResponse();
            else if (this.msg_info[2].charAt(0) == 'g')
                this.getResponse();
            else
                System.out.println("Invalid command passed to responder.");
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
                this.choose_response();
			} catch (IOException e) {
				e.printStackTrace();
			}
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
}
