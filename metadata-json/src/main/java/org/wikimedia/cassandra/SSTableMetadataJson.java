package org.wikimedia.cassandra;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class SSTableMetadataJson {

    private static ObjectMapper mapper = new ObjectMapper();

    static {
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    public static void main(String... args) throws IOException {

        if (args.length == 0) {
            System.err.printf("Usage: %s <file> [...]%n", SSTableMetadataJson.class.getSimpleName());
            System.exit(1);
        }

        List<SSTableMetadata> objects = new ArrayList<>();

        for (String fname : args) {
            if (new File(fname).exists()) {
                objects.add(SSTableMetadata.fromFile(fname));
            }
            else {
                out("No such file: %s", fname);
            }
        }

        out(mapper.writeValueAsString(objects));
    }

    private static void out(String format, String... args) {
        System.out.printf(format + "%n", (Object[]) args);
    }

}
