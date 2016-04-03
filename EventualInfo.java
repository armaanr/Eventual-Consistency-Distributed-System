
public class EventualInfo
{
    String variable;
    int value;
    int timestamp;

    public EventualInfo(String variable, int value, int timestamp)
    {
        this.variable = variable;
        this.value = value;
        this.timestamp = timestamp;
    }
    
    /*
     * Updates information if given timestamp is newer.
     * Breaks ties with replica of lower id winning.
     */
    public void update(int new_value, int new_timestamp, int my_id, int other_id)
    {
        if ((this.timestamp < new_timestamp)
            || ((this.timestamp == new_timestamp) && (my_id < other_id)))
        {
            this.value = new_value;
            this.timestamp = new_timestamp;
        }
    }
}
