import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.VectorSchemaRoot;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;


class LanceReader {

    // This is the code of arrow reader, referring to this to design API for now
    private static arrow_reader(){
        File file = new File("./thirdpartydeps/arrowfiles/random_access.arrow");
        try(
            BufferAllocator rootAllocator = new RootAllocator();
            FileInputStream fileInputStream = new FileInputStream(file);
            ArrowFileReader reader = new ArrowFileReader(fileInputStream.getChannel(), rootAllocator)
        ){
            System.out.println("Record batches in file: " + reader.getRecordBlocks().size());
            for (ArrowBlock arrowBlock : reader.getRecordBlocks()) {
                reader.loadRecordBatch(arrowBlock);
                VectorSchemaRoot vectorSchemaRootRecover = reader.getVectorSchemaRoot();
                System.out.print(vectorSchemaRootRecover.contentToTSVString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // If we mimic arrow API, we need to solve a lot of typing/inheritance stuff
    private static native List<ArrowBlock> getRecordBlocks();
    private static native boolean loadRecordBatch(ArrowBlock block);
    private static native VectorSchemaRoot getVectorSchemaRoot();


    // Could be easier if we just start from scrath


    public static void main(String[] args) {
        System.out.println("Hello, World!");
    }
}
