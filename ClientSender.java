import java.net.*;
import java.util.*;
import java.io.*;

/*
 * Sends a message in a new thread to the client's replica server.
 */
public class ClientSender implements Runnable
{
    public String cmd;
    public Client client;

    /*
     * Creates a runnable class that makes a new thread
     * for each command that is sent.
     */
    public ClientSender(Client client, String cmd)
    {
        this.client = client;
        this.cmd = cmd;
    }

    /* 
     * Sends a cmd to the assigned replica server.
     */
    private void send_cmd() throws IOException 
    {
        InetAddress destIp = this.client.replica_ip;
        int destPort = this.client.replica_port;
           
        Socket sendSock = new Socket(destIp, destPort);	   
        DataOutputStream out = new DataOutputStream(sendSock.getOutputStream());
        
        out.writeUTF(this.cmd +  " " + this.client.receiving_port);
        
        sendSock.close();
    }

    /*
     * Runs a thread for each cmd that is sent to the replica.
     */
    public void run()
    {
        try {
            if (Thread.interrupted())
            {
                try {
                    throw new InterruptedException();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            send_cmd();
        }
        catch (ConnectException c) {
            System.out.println("Switching to server with next higher id.");
            this.client.reset(cmd);
        }
        catch (IOException e) {
             e.printStackTrace();
        }
    }
}