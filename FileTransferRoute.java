import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.main.Main;

public class FileTransferRoute extends RouteBuilder {
    public static void main(String[] args) throws Exception {
        Main main = new Main();
        main.configure().addRoutesBuilder(new FileTransferRoute());
        main.run();
    }

    @Override
    public void configure() throws Exception {

        from("file:input?noop=true")
                .filter(header("CamelFileName").endsWith(".csv"))
                .log("Procesando archivo: ${file:name} a las ${date:now:yyyy-MM-dd HH:mm:ss}")
                .convertBodyTo(String.class)
                .transform().simple("${body.toUpperCase()}")
                .to("file:output")
                .to("file:archived");
    }
}