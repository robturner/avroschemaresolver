import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.junit.Test;


public class TestBytes {

  @Test
  public void test() throws IOException {
    Schema byteBlobSchema = Schema.create(Schema.Type.BYTES); 
    File path = new File("src/test/resources/test.avro");

    GenericDatumWriter<ByteBuffer> wdata = new GenericDatumWriter<ByteBuffer>();
    DataFileWriter<ByteBuffer> dataFileWriter = new DataFileWriter<ByteBuffer>(wdata);
    dataFileWriter.create(byteBlobSchema, new FileOutputStream(path));
    dataFileWriter.append(ByteBuffer.wrap(new String("Hello").getBytes()));
    dataFileWriter.close();
    
    GenericDatumReader<ByteBuffer> rdata = new GenericDatumReader<ByteBuffer>(byteBlobSchema);

    DataFileReader<ByteBuffer> dataFileReader = new DataFileReader<ByteBuffer>(path, rdata);

    ByteBuffer b = null;
    
    while(dataFileReader.hasNext()) {

           b = dataFileReader.next(b);
           byte[] result = new byte[b.remaining()];
           b.get(result);
           System.out.println(new String(result));
    }
  }

}
