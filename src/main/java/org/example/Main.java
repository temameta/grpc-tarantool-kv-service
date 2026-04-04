package org.example;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import lombok.extern.slf4j.Slf4j;
import org.example.kv.KvServiceImpl;

@Slf4j
public class Main {
    private final static String SPACE_NAME = "KV";
    public static void main(String[] args) throws Exception {
        try (TarantoolBoxClient tarantoolBoxClient = TarantoolFactory.box()
                .withHost("127.0.0.1")
                .withPort(3301)
                .build();
        ) {
            String initScript = """
                        local space = box.schema.space.create('KV', {if_not_exists = true})
                        space:format({
                            {name = 'key', type = 'string'},
                            {name = 'value', type = 'varbinary', is_nullable = true}
                        })
                        space:create_index('primary', {
                            parts = {{field = 'key'}},
                            if_not_exists = true
                        })
                    """;
            tarantoolBoxClient.eval(initScript).join();
            Server server = ServerBuilder
                    .forPort(9090)
                    .addService(new KvServiceImpl(tarantoolBoxClient, SPACE_NAME))
                    .build();
            server.start();
            log.info("Сервер запущен на порту 9090");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Остановка сервера...");
                server.shutdown();
            }));
            server.awaitTermination();
        }

    }
}
