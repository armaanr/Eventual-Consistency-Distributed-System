import java.util.*;

/*
 * Object for holding all information for a client's request.
 */
public class MessageInfo
{
    int remaining;
    int client_id;
    int value;
    int timestamp;
    String variable;
    char[] message;
    List<Integer> values;

    public MessageInfo(int remaining, int client_id, char[] message, int value, int timestamp, String variable)
    {
        this.remaining = remaining;
        this.client_id = client_id;
        this.message = message;
        this.value = value;
        this.timestamp = timestamp;
        this.variable = variable;
    }
}
