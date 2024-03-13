package de.intranda.goobi;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class PluginInfo {

    public static void main(String[] args) throws FileNotFoundException, IOException {
        System.out.println("+++++++++++++++++++++++++++++++++++++++++++++++++++++\n");
        System.out.println("This is a Goobi plugin from intranda");
        System.out.println("Copyright 2018 by intranda GmbH");
        System.out.println("All rights reserved");
        System.out.println("info@intranda.com");
        System.out.println("\n-----------------------------------------------------\n");
        System.out.println("This plugin file contains the following plugins:");

        String bla = convertStreamToString(PluginInfo.class.getResourceAsStream("plugins.txt"));
        bla = bla.replaceAll(".class", "");
        bla = bla.replaceAll(" ", "\n");
        System.out.println("\n" + bla);

        System.out.println("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++");
    }

    public static String convertStreamToString(InputStream is) throws IOException {
        StringBuilder sb = new StringBuilder(Math.max(16, is.available()));
        char[] tmp = new char[4096];

        try {
            InputStreamReader reader = new InputStreamReader(is);
            for (int cnt; (cnt = reader.read(tmp)) > 0;) {
                sb.append(tmp, 0, cnt);
            }
        } finally {
            is.close();
        }
        return sb.toString();
    }

}
