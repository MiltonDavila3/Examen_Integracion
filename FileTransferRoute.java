import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Set;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;

public class FileTransferRoute extends RouteBuilder {
    private static final String EXPECTED_HEADER = "patient_id,full_name,appointment_date,insurance_code";
    private static final Set<String> VALID_INSURANCE = Set.of("IESS", "PRIVADO", "NINGUNO");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter
            .ofPattern("M/d/uuuu")
            .withResolverStyle(ResolverStyle.STRICT);

    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.configure().addRoutesBuilder(new FileTransferRoute());
        main.run();
    }

    @Override
    public void configure() throws Exception {
        from("file:input?readLock=changed")
                .filter(header("CamelFileName").endsWith(".csv"))
                .log("Procesando archivo: ${file:name} a las ${date:now:yyyy-MM-dd HH:mm:ss}")
                .convertBodyTo(String.class)
                .doTry()
                .process(this::validateCsv)
                .to("file:output")
                .setHeader(Exchange.FILE_NAME,
                        simple("${file:name.noext}_${date:now:yyyy-MM-dd_HHmmss}.${file:ext}"))
                .to("file:archive")
                .log("Archivo valido: ${file:name} enviado a output y archivado como ${header.CamelFileName}")
                .doCatch(IllegalArgumentException.class)
                .log(LoggingLevel.WARN,
                        "Archivo invalido: ${file:name}. Motivo: ${exception.message}")
                .setHeader(Exchange.FILE_NAME,
                        simple("${file:name.noext}_${date:now:yyyy-MM-dd_HHmmss}.${file:ext}"))
                .to("file:error")
                .log(LoggingLevel.WARN,
                        "Archivo invalido enviado a error como ${header.CamelFileName}")
                .end();
    }

    private void validateCsv(Exchange exchange) {
        String body = exchange.getIn().getBody(String.class);

        if (body == null || body.trim().isEmpty()) {
            throw new IllegalArgumentException("El archivo esta vacio");
        }

        String[] lines = body.split("\\r?\\n");
        if (lines.length < 2) {
            throw new IllegalArgumentException("Debe incluir encabezado y al menos una fila de datos");
        }

        String header = lines[0].trim();
        if (!EXPECTED_HEADER.equals(header)) {
            throw new IllegalArgumentException("Header invalido. Debe ser: " + EXPECTED_HEADER);
        }

        for (int i = 1; i < lines.length; i++) {
            String line = lines[i].trim();
            if (line.isEmpty()) {
                throw new IllegalArgumentException("Fila " + (i + 1) + " vacia");
            }

            String[] cols = line.split(",", -1);
            if (cols.length != 4) {
                throw new IllegalArgumentException("Fila " + (i + 1) + " debe tener exactamente 4 columnas");
            }

            String patientId = cols[0].trim();
            String fullName = cols[1].trim();
            String appointmentDate = cols[2].trim();
            String insurance = cols[3].trim();

            if (patientId.isEmpty() || fullName.isEmpty() || appointmentDate.isEmpty() || insurance.isEmpty()) {
                throw new IllegalArgumentException("Fila " + (i + 1) + " tiene campos vacios");
            }

            if (!patientId.matches("\\\\d+")) {
                throw new IllegalArgumentException("Fila " + (i + 1) + ": patient_id debe ser numerico");
            }

            try {
                LocalDate.parse(appointmentDate, DATE_FORMATTER);
            } catch (DateTimeParseException ex) {
                throw new IllegalArgumentException("Fila " + (i + 1)
                        + ": appointment_date invalida. Formato esperado M/d/yyyy (ej: 5/13/2026)");
            }

            if (!VALID_INSURANCE.contains(insurance)) {
                throw new IllegalArgumentException("Fila " + (i + 1)
                        + ": insurance_code invalido. Valores permitidos: IESS, PRIVADO, NINGUNO");
            }
        }
    }
}