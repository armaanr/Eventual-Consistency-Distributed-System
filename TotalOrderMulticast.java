import java.util.ArrayList;

public class TotalOrderMulticast {
	
	public class TotalOrderInfo
	{
		public String message;
		public int currGlobalSeq;
		int id;

		
		public TotalOrderInfo(String msg, int global, int ID)
		{
			this.message = msg;
			this.currGlobalSeq = global;
			this.id = ID;
		}
	
	}
	
	public ArrayList<TotalOrderInfo> seqBuffer;
	public ArrayList<TotalOrderInfo> recMessages;
	public int localSequence;
	
	public TotalOrderMulticast()
	{
		this.seqBuffer = new ArrayList<TotalOrderInfo>();
		this.recMessages = new ArrayList<TotalOrderInfo>();
		this.localSequence = 0;
		
	}
	
	public int messagesCheck(String msg, int id)
	{
		System.out.println("looking for id => "+ id +" and message => "+ msg+"in resmessages");
		for(TotalOrderInfo curr : recMessages )
		{
			
			if(curr.message.equals(msg) && curr.id == id)
			{
				System.out.println("found in rec");
				return recMessages.indexOf(curr);
			}
		}
		
		return -1;
	}
	
	public int sequencerCheck(String msg, int id, int ls)
	{
		System.out.println("looking for id => "+ id +" and message => "+ msg+"in seqBuffer");
		for(TotalOrderInfo curr : seqBuffer)
		{
			
			if(curr.message.equals(msg) && (curr.currGlobalSeq == ls+1) )
			{
				System.out.println("found in seq");
				return seqBuffer.indexOf(curr);
			}
		}
		
		return -1;
	}
	
	

}
