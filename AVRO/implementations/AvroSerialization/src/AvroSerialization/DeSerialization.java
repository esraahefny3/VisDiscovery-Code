
package AvroSerialization;

import java.io.File;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;

/**
 *
 * @author Nada Essam
 */
public class DeSerialization {
    
    String AvroPath, SchemaPath;
    
    public DeSerialization(String in, String schema) {
        AvroPath = in;
        SchemaPath = schema;
    }
    
    public void Deserialize () throws IOException {
		// create a schema
		Schema schema = new Schema.Parser().parse(new File(SchemaPath));
		// create a record using schema
		GenericRecord AvroRec = new GenericData.Record(schema);
		File AvroFile = new File(AvroPath);
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(AvroFile, datumReader);
		System.out.println("Deserialized data is :");
		while (dataFileReader.hasNext()) {
			AvroRec = dataFileReader.next(AvroRec);
			System.out.println(AvroRec);
		}
	}
    
}
