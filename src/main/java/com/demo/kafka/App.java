package com.demo.kafka;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        
        String str = "adsf^Aad^Asfa^Afas";
        String[] split = str.split("\\^A");
        System.out.println(split.length);
    }
}
