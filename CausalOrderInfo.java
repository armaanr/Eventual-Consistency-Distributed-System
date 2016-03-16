
public class CausalOrderInfo {
	
	public String message;
	String mClock ;
	int port;

	
	public CausalOrderInfo(String msg,String clock,int prt)
	{
		this.message = msg;
		this.mClock = clock;
		this.port = prt;
	}
	
	
	
}
