package com.g10.CPEN431.A11;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;

public class App {
    public static void main(String[] args) {
        if (args.length != 5) {
            System.out.println(
                    "Enter the server ip, port number, server-list, filename, number of nodes, expiration time as arguments");
            return;
        }

        InetAddress nodeIp = null;
        int nodePort;
        String nodeIpStr = args[0];
        String nodePortStr = args[1];
        String filepath = args[2];
        String NUM_NODES = args[3];
        String EXP_TIME = args[4];
        List<Pair<InetAddress, Integer>> allAddresses = new ArrayList<>();

        try {
            nodeIp = InetAddress.getByName(nodeIpStr);
            nodePort = Integer.parseInt(nodePortStr);
            System.out.println("Server Address: " + nodeIp.getHostName() + ":" + nodePort);
            BufferedReader reader = new BufferedReader(new FileReader(filepath));
            String line;

            while ((line = reader.readLine()) != null) {
                String[] addrInfo = line.split(":");
                InetAddress serverIp = InetAddress.getByName(addrInfo[0]);
                int serverPort = Integer.parseInt(addrInfo[1]);
                allAddresses.add(new Pair<InetAddress, Integer>(serverIp, serverPort));
            }
            reader.close();
        } catch (NumberFormatException e) {
            System.err.println("Passing invalid value for port");
            return;
        } catch (FileNotFoundException e) {
            System.err.println("File " + filepath + " not found");
            return;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Router router = new Router(allAddresses);
        try {
            Server server = new Server(nodeIp, nodePort, router, Integer.parseInt(NUM_NODES),
                    Integer.parseInt(EXP_TIME));
            server.listen();
        } catch (SocketException e) {
            e.printStackTrace();
            System.exit(1);
        } catch (NumberFormatException e) {
            System.err.println("Passing invalid value for number of nodes or expiration time");
            return;
        }
    }
}