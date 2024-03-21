package com.example;

import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.c.Data;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class LanceReader {
    private static native long[] readRangeJni(String path, int start, int end);

    private static native long[] readIndexJni(String path, int[] indices);

    static {
        System.loadLibrary("lanceio");
    }

    public static ArrowRecordBatch readIndex(String path, int[] indices) {
        // Assuming you have a jobjectArray called result
        long[] result = readIndexJni(path, indices);
        List<FieldVector> vec = new ArrayList<>();

        // Get the length of the array
        int length = result.length;

        // Iterate over the array and print the values
        for (int i = 0; i < length; i += 2) {
            BufferAllocator allocator = new RootAllocator();

            ArrowSchema arrowSchema = ArrowSchema.wrap(result[i]);
            ArrowArray array = ArrowArray.wrap(result[i + 1]);

            vec.add(Data.importVector(allocator, array, arrowSchema, null));

        }

        // Build the vector schema root
        VectorSchemaRoot root = new VectorSchemaRoot((List<FieldVector>) vec);
        // Load ArrowRecordBatch from root
        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch recordBatch = unloader.getRecordBatch();
        return recordBatch;
    }

    public static ArrowRecordBatch readRange(String path, int start, int end) {
        // Assuming you have a jobjectArray called result
        long[] result = readRangeJni(path, start, end);
        List<FieldVector> vec = new ArrayList<>();

        // Get the length of the array
        int length = result.length;

        // Iterate over the array and print the values
        for (int i = 0; i < length; i += 2) {
            BufferAllocator allocator = new RootAllocator();

            ArrowSchema arrowSchema = ArrowSchema.wrap(result[i]);
            ArrowArray array = ArrowArray.wrap(result[i + 1]);

            vec.add(Data.importVector(allocator, array, arrowSchema, null));

        }

        // Build the vector schema root
        VectorSchemaRoot root = new VectorSchemaRoot((List<FieldVector>) vec);

        // Print the contents of VectorSchemaRoot
        System.out.println("Vector Schema Root:");
        System.out.println(root);

        // Load ArrowRecordBatch from root
        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch recordBatch = unloader.getRecordBatch();

        return recordBatch;
    }

    // The rest is just regular ol' Java!
    public static void main(String[] args) {
        String base = "/home/scott/lance-jni-bridge/";
        readRange(base + "test_frag", 0, 10);
    }
}

class LanceWriter {

    private static native void write(String path, ArrowRecordBatch rb);

    static {
        System.loadLibrary("lanceio");
    }
}
