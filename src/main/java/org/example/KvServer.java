package org.example;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import lombok.extern.slf4j.Slf4j;
import org.example.kv.KvServiceImpl;

@Slf4j
public class KvServer {
    private final static String SPACE_NAME = "KV";
    private final static String TARANTOOL_HOST = "127.0.0.1";
    private final static int TARANTOOL_PORT = 3301;
    private final static int GRPC_PORT = 9090;
    public static void main(String[] args) {
        log.info("Starting application");
        log.info("Connecting to Tarantool on {}:{}", TARANTOOL_HOST, TARANTOOL_PORT);
        try (TarantoolBoxClient tarantoolBoxClient = TarantoolFactory.box()
                .withHost(TARANTOOL_HOST)
                .withPort(TARANTOOL_PORT)
                .build()
        ) {
            log.info("Connection to Tarantool successful");
            String initScript = String.format("""
                        local space = box.schema.space.create('%s', {if_not_exists = true})
                        space:format({
                            {name = 'key', type = 'string'},
                            {name = 'value', type = 'varbinary', is_nullable = true}
                        })
                        space:create_index('primary', {
                            parts = {{field = 'key'}},
                            if_not_exists = true
                        })
                    """, SPACE_NAME);
            log.info("Executing init script if not initialized yet");
            tarantoolBoxClient.eval(initScript).join();
            log.info("Init successful");
            log.info("Creating connection for gRPC on port {}", GRPC_PORT);
            Server server = ServerBuilder
                    .forPort(GRPC_PORT)
                    .addService(new KvServiceImpl(tarantoolBoxClient, SPACE_NAME))
                    .build();
            log.info("Creation successful. Starting server");
            server.start();
            log.info("Server started on port {}", GRPC_PORT);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown server");
                server.shutdown();
            }));
            server.awaitTermination();
        } catch (Exception e) {
            log.error("Fatal error: ", e);
            System.exit(1);
        }
    }
}
