import java.util.ArrayList;

public class CausalOrderMulticaster {
	
	public int clock[];
	ArrayList<CausalOrderInfo> buffer;
	
	public CausalOrderMulticaster()
	{
		clock = new int[4];
		
		for(int i = 0; i< clock.length; i++)
	      {
	    	  clock[i] = 0;
	      }
		
		buffer = new ArrayList<CausalOrderInfo>();
	}
	
	public String toString()
	{
		return clock[0]+","+clock[1]+","+clock[2]+","+clock[3];
	}
	
	public int[] strToArr(String str)
	{
		String vector[] = str.split(",");  
		int newClock[] = new int[4];
		for(int i = 0; i< 4; i++)
		{
			newClock[i] = Integer.parseInt(vector[i]);
		}
		return newClock;
	}

}
