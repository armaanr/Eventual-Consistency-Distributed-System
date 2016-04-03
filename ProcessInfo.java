import java.io.IOException;
import java.net.*;

//Stores process information
public class ProcessInfo {
	private int port;
	private InetAddress ip;
    public boolean alive;
	
	public ProcessInfo(InetAddress ip, int port) throws IOException{
		this.ip = ip;
		this.port = port;
        this.alive = true;
	}
	
	int getPort(){
		return this.port;
	}
	
	InetAddress getIP(){
		return this.ip;
	}

}
