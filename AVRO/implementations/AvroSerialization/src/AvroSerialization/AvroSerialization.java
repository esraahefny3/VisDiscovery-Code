package AvroSerialization;

/**
 *
 * @author Nada Essam
 */


import java.io.IOException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;


public class AvroSerialization {
    
   

    public static void main(String[] args) throws JsonParseException, JsonProcessingException, IOException{
        
        
        Serialization SerializingToAvro= new Serialization("resources/user.json","resources/user.avsc");
        SerializingToAvro.serialize();
        
        DeSerialization DeSerializingToAvro= new DeSerialization("resources/user.avro","resources/user.avsc");
        DeSerializingToAvro.Deserialize();
    }
    
}
