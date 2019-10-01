package client;


import java.util.Scanner;

public class InteractiveClient {
    public static final String EXIT = "exit";
    public static final String SPACE = " ";
    public static final String SUBSCRIBE = "subscribe";
    public static final String UNSUBSCRIBE = "unsubscribe";
    public static final String PUBLISH = "publish";
    public static final String HELP = "help";
    public static final String HELP_TEXT = "subscribe <topic> - subscribe to a topic.\n" +
            "unsubscribe <topic> - unsubscribe to a topic.\n" +
            "publish <topic> <message> - publish a message in the given topic.\n" +
            "exit - shutdown the client.";
    public static final String SUBSCRIBE_TEXT = "Subscribing: %s\n";
    public static final String UNSUBSCRIBE_TEXT = "Unsubscribing: %s\n";
    public static final String PUBLISH_TEXT = "Publishing in topic %s: %s\n";
    public static final String INVALID_COMMAND = "Invalid command type help.";
    public static String EMPTY_STRING = "";

    public static void main(String[] args) throws Exception {
        Client c = new Client(args);
        Scanner in = new Scanner(System.in);
        c.subscribe("a");
        String input = EMPTY_STRING;
        while (!input.equalsIgnoreCase(EXIT)) {
            input = in.nextLine();
            String[] input_array = input.split(SPACE);
            String command = input_array[0],
                    topic = input_array.length >= 2 ? input_array[1] : EMPTY_STRING,
                    message = input_array.length >= 3 ? input_array[2] : EMPTY_STRING;
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
                    System.out.printf(PUBLISH_TEXT, topic, message);
                    c.publish(topic, message);
                    break;
                case HELP:
                    System.out.println(HELP_TEXT);
                    break;
                default:
                    System.out.println(INVALID_COMMAND);
            }
        }
    }
}
