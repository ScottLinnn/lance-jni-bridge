package jni;

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
import org.apache.arrow.vector.*;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class LanceReader {

    public static void printFieldVector(FieldVector vector) {
        int valueCount = vector.getValueCount();

        System.out.println("Printing FieldVector data:");

        for (int i = 0; i < valueCount; i++) {
            if (vector.isNull(i)) {
                System.out.println("null");
            } else {
                // Depending on the data type of the vector, you'll need to use different
                // methods
                if (vector instanceof IntVector) {
                    System.out.println(((IntVector) vector).get(i));
                } else if (vector instanceof Float8Vector) {
                    System.out.println(((Float8Vector) vector).get(i));
                } else if (vector instanceof VarCharVector) {
                    System.out.println(((VarCharVector) vector).getObject(i).toString());
                } else {
                    // Handle other data types as needed
                    System.out.println("Unsupported data type");
                }
            }
        }
    }

    static {
        System.load("/home/scott/lance-jni-bridge/lanceio_rust/target/debug/liblanceio_jni.so");
        System.loadLibrary("lanceio_jni");
    }

    private static native String hello(String input);

    private static native long[] readRangeJni(String path, int start, int end);

    private static native long[] readIndexJni(String path, int[] indices);

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

            FieldVector fieldVector = Data.importVector(allocator, array, arrowSchema, null);
            System.out.println("readIndex, printing FieldVector data, i = " + i);
            printFieldVector(fieldVector);
            vec.add(fieldVector);

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
            FieldVector fieldVector = Data.importVector(allocator, array, arrowSchema, null);
            System.out.println("readRange, printing FieldVector data, i = " + i);
            printFieldVector(fieldVector);
            vec.add(fieldVector);

        }

        // Build the vector schema root
        VectorSchemaRoot root = new VectorSchemaRoot((List<FieldVector>) vec);

        // Load ArrowRecordBatch from root
        VectorUnloader unloader = new VectorUnloader(root);
        ArrowRecordBatch recordBatch = unloader.getRecordBatch();

        return recordBatch;
    }

    // The rest is just regular ol' Java!
    public static void main(String[] args) {
        System.out.println(hello("hello from Java!"));
        String base = "/home/scott/lance-jni-bridge/";
        readRange(base + "test_dataset", 0, 40);
        readRange(base + "test_dataset", 20, 30);
        int[] indices = { 24, 4, 15, 6, 26 };
        readIndex(base + "test_dataset", indices);
    }
}

class LanceWriter {

    private static native void write(String path, ArrowRecordBatch rb);

    static {
        System.loadLibrary("lanceio_jni");
    }
}
