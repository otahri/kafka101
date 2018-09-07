package generator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Random;

public class EventGenerator {

    private Schema schema;

    public EventGenerator(Schema schema) {
        this.schema = schema;
    }

    public GenericRecord generate() {

        List<Schema.Field> fields = schema.getFields();

        GenericRecord genericRecord = new GenericData.Record(schema);

        fields.forEach(field -> putInRecord(genericRecord, field.name(), field.schema().getType()));

        return genericRecord;
    }

    private void putInRecord(GenericRecord record, String field, Schema.Type type) {

        Random random = new Random();
        RandomString randomString = new RandomString(5,random);

        switch (type) {
            case STRING:
                record.put(field, randomString.nextString());
                break;
            case INT:
                record.put(field, random.nextInt(100));
                break;
            case LONG:
                record.put(field, random.nextLong());
                break;
            case DOUBLE:
                record.put(field, random.nextDouble());
                break;
            case FLOAT:
                record.put(field, random.nextFloat());
                break;
        }
    }
}
