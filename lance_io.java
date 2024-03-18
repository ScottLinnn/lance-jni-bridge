package com.example;


import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import java.util.List;

class LanceReader {
    private static native List<long []> readRangeJni(String dir,int start, int end);
    private static native List<long []> readIndexJni(String dir,int[] indices);
    static {
        System.loadLibrary("lanceio");
    }

    public static void main(String[] args) {
        
        
    }
}

class LanceWriter {

    private static native void write(String dir,ArrowRecordBatch rb);
    static {
        ArrowRecordBatch rb = null;
        LanceWriter.write("data", rb);
    }
}
