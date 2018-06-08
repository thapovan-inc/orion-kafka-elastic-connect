package com.thapovan.kafka;

import java.util.Calendar;

public class ProcessIncompleteRecords
{
    public static void main(String args[])
    {
        Calendar now = Calendar.getInstance();

        System.out.println("CURRENT TIME: " + now.getTime());

        now.add(Calendar.MINUTE, -1);

        System.out.println("BEFORE 1 MIN: " + now.getTime());
    }
}
