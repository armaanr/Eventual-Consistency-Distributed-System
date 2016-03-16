
//Node that is used to store messages in the message buffer
public class TotalOrderInfo
{
		public String message;
		public int globalSequence;
		int port;

		
		public TotalOrderInfo(String msg, int global, int prt)
		{
			this.message = msg;
			this.globalSequence = global;
			this.port = prt;
		}
	
}