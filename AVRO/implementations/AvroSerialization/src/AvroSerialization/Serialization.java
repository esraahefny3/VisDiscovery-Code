package AvroSerialization;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;

/**
 *
 * @author Nada Essam
 */
public class Serialization {

    String DataPath, SchemaPath;

    public Serialization(String in, String schema) {
        DataPath = in;
        SchemaPath = schema;
    }

    public void serialize() throws FileNotFoundException, IOException {

        InputStream in = new FileInputStream(DataPath);
        // create a schema
        Schema schema = new Schema.Parser().parse(new File(SchemaPath));
        // create a record to hold json
        GenericRecord AvroRec = new GenericData.Record(schema);
        // this file will have AVro output data
        File AvroFile = new File("resources/user.avro");
        // Create a writer to serialize the record
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        dataFileWriter.create(schema, AvroFile);
        // get fields name and put it in arraylist
        List<String> fieldValues = new ArrayList<>();
        for (Field field : schema.getFields()) {
            fieldValues.add(field.name());
        }
        // iterate over JSONs present in input file and write to Avro output file
        for (Iterator it = new ObjectMapper().readValues(new JsonFactory().createJsonParser(in), JSONObject.class);
                it.hasNext();) {
            JSONObject JsonRec = (JSONObject) it.next();
            // put values of fields on avro record
            for (String value : fieldValues) {
                AvroRec.put(value, JsonRec.get(value));
            }
            dataFileWriter.append(AvroRec);
        }  // end of for loop

        in.close();
        dataFileWriter.close();
    }
}
