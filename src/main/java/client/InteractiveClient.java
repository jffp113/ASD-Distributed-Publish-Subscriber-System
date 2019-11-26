package client;


import java.util.Scanner;

public class InteractiveClient {

    private static final String EXIT = "exit";
    private static final String SPACE = " ";
    private static final String SUBSCRIBE = "subscribe";
    private static final String UNSUBSCRIBE = "unsubscribe";
    private static final String PUBLISH = "publish";
    private static final String HELP = "help";
    private static final String HELP_TEXT = "subscribe <topic> - subscribe to a topic.\n" +
            "unsubscribe <topic> - unsubscribe to a topic.\n" +
            "publish <topic> <message> - publish a message in the given topic.\n" +
            "exit - shutdown the client.";
    private static final String SUBSCRIBE_TEXT = "Subscribing: %s\n";
    private static final String UNSUBSCRIBE_TEXT = "Unsubscribing: %s\n";
    private static final String PUBLISH_TEXT = "Publishing in topic %s: %s\n";
    private static final String INVALID_COMMAND = "Invalid command type help.";
    public static final String INVALID_NUMBER_OF_ARGUMENTS = "Invalid number of arguments";
    private static String EMPTY_STRING = "";

    public static void main(String[] args) throws Exception {
        Client c = new Client(args);
        Scanner in = new Scanner(System.in);
        String input = EMPTY_STRING;
        while (!input.equalsIgnoreCase(EXIT)) {
            input = in.nextLine();
            String[] input_array = input.split(SPACE);
            String command = input_array[0],
                    topic = input_array.length >= 2 ? input_array[1] : EMPTY_STRING;
            switch (command) {
                case SUBSCRIBE:
                    System.out.printf(SUBSCRIBE_TEXT, topic);
                    c.subscribe(topic);
                    break;
                case UNSUBSCRIBE:
                    System.out.printf(UNSUBSCRIBE_TEXT, topic);
                    c.unsubscribe(topic);
                    break;
                case PUBLISH:
                    if(input_array.length < 3) {
                        System.out.println(INVALID_NUMBER_OF_ARGUMENTS);
                        break;
                    }
                    System.out.printf(PUBLISH_TEXT, topic, getMessage(input_array));
                    c.publish(topic, getMessage(input_array));
                    break;
                case HELP:
                    System.out.println(HELP_TEXT);
                    break;
                default:
                    System.out.println(INVALID_COMMAND);
            }
        }
    }

    private static String getMessage(String[] input_array){
        StringBuffer buf = new StringBuffer();

        buf.append(input_array[2]);

        for(int i = 3; i < input_array.length; i++){
            buf.append(SPACE);
            buf.append(input_array[i]);
        }

        return buf.toString();
    }
}
