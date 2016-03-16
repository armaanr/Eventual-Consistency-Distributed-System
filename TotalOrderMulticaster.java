import java.util.ArrayList;

//contains the variables required for Total Order Implementation
public class TotalOrderMulticaster {
	
	public ArrayList<TotalOrderInfo> buffer;
	int localsequence;
	
	public TotalOrderMulticaster()
	{
		this.buffer = new ArrayList<TotalOrderInfo>();
		this.localsequence = 0;
	}
	
	public TotalOrderInfo pop()
	{
		if(!this.buffer.isEmpty())
		{
			return buffer.remove(this.buffer.size() -1);
		}
		
		return null;
	}
	

}
