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
	
	public TotalOrderInfo pop()
	{
		if(!this.seqBuffer.isEmpty())
		{
			return seqBuffer.remove(this.seqBuffer.size() -1);
		}
		
		return null;
	}
	
	

}
