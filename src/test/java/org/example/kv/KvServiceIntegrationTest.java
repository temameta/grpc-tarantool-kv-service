package org.example.kv;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.tarantool.client.box.TarantoolBoxClient;
import io.tarantool.client.factory.TarantoolFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import com.google.protobuf.ByteString;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class KvServiceIntegrationTest {
    private static final String SPACE_NAME = "KV";
    private static final String TARANTOOL_HOST = "127.0.0.1";
    private static final int TARANTOOL_PORT = 3301;
    private TarantoolBoxClient tarantoolBoxClient;
    private Server inProcessServer;
    private ManagedChannel inProcessChannel;
    private KvServiceGrpc.KvServiceBlockingStub stub;

    @BeforeEach
    void setUp() throws Exception {
        tarantoolBoxClient = TarantoolFactory.box()
                .withHost(TARANTOOL_HOST)
                .withPort(TARANTOOL_PORT)
                .build();
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
        tarantoolBoxClient.eval(initScript).join();
        tarantoolBoxClient.eval("box.space." + SPACE_NAME + ":truncate()").join();
        String serverName = InProcessServerBuilder.generateName();
        inProcessServer = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new KvServiceImpl(tarantoolBoxClient, SPACE_NAME))
                .build()
                .start();
        inProcessChannel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();
        stub = KvServiceGrpc.newBlockingStub(inProcessChannel);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (inProcessChannel != null) {
            inProcessChannel.shutdownNow();
            inProcessChannel.awaitTermination(1, TimeUnit.SECONDS);
        }
        if (inProcessServer != null) {
            inProcessServer.shutdownNow();
            inProcessServer.awaitTermination(1, TimeUnit.SECONDS);
        }
        if (tarantoolBoxClient != null) {
            tarantoolBoxClient.close();
        }
    }

    @Test
    @DisplayName("The value should be successfully put into the database and returned from there")
    void shouldPutAndGetSuccessfully() {
        String key = "test_key";
        byte[] value = "test_value".getBytes(StandardCharsets.UTF_8);
        PutRequest putRequest = PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFrom(value))
                .build();
        PutResponse putResponse = stub.put(putRequest);
        assertTrue(putResponse.getSuccess(), "Expected true in put response");
        GetRequest getRequest = GetRequest.newBuilder()
                .setKey(key)
                .build();
        GetResponse getResponse = stub.get(getRequest);
        assertTrue(getResponse.hasValue(), "Expected not null entry in database");
        assertArrayEquals(value, getResponse.getValue().toByteArray(), "Current value and value in database must match");
    }

    @Test
    @DisplayName("The value should be deleted successfully from database")
    void shouldDeleteExistingKey() {
        String key = "key_to_delete";
        stub.put(PutRequest.newBuilder()
                .setKey(key)
                .setValue(ByteString.copyFromUtf8("some_data"))
                .build());
        DeleteResponse deleteResponse = stub.delete(DeleteRequest.newBuilder().setKey(key).build());
        assertTrue(deleteResponse.getSuccess(), "Expected true in delete response");
        StatusRuntimeException exception = assertThrows(
                StatusRuntimeException.class,
                () -> stub.get(GetRequest.newBuilder().setKey(key).build()),
                "Expected error from request on non-existing key"
        );
        assertEquals(io.grpc.Status.Code.NOT_FOUND, exception.getStatus().getCode(), "Expected NOT_FOUND error code for non-existing key");
    }

    @Test
    @DisplayName("Should return accurate count of entries in database")
    void shouldCountRecordsAccurately() {
        assertEquals(0, stub.count(Empty.getDefaultInstance()).getCount(), "Expected true for 0 entries check");
        for (int i = 1; i <= 3; i++) {
            stub.put(PutRequest.newBuilder()
                    .setKey("key_" + i)
                    .setValue(ByteString.copyFromUtf8("value"))
                    .build());
        }
        CountResponse countResp = stub.count(Empty.getDefaultInstance());
        assertEquals(3, countResp.getCount(), "Expected 3 entries");
    }

    @Test
    @DisplayName("Should return accurate stream of entries")
    void shouldReturnRangeStream() {
        String[] keys = {"A", "B", "C", "D", "E", "F"};
        for (String key : keys) {
            stub.put(PutRequest.newBuilder()
                    .setKey(key)
                    .setValue(ByteString.copyFromUtf8("value_for_" + key))
                    .build());
        }
        RangeRequest request = RangeRequest.newBuilder()
                .setKeySince("B")
                .setKeyTo("E")
                .build();
        Iterator<KeyValuePair> responseStream = stub.range(request);
        List<KeyValuePair> results = new ArrayList<>();
        while (responseStream.hasNext()) {
            results.add(responseStream.next());
        }
        assertEquals(4, results.size(), "Expected 4 entries in range [B, E]");
        assertEquals("B", results.get(0).getKey(), "Expected first key B");
        assertEquals("C", results.get(1).getKey(), "Expected second key C");
        assertEquals("D", results.get(2).getKey(), "Expected third key D");
        assertEquals("E", results.get(3).getKey(), "Expected fourth key E");
        assertEquals("value_for_B", results.get(0).getValue().toStringUtf8(), "Expected first value value_for_B");
        assertEquals("value_for_C", results.get(1).getValue().toStringUtf8(), "Expected second value value_for_C");
        assertEquals("value_for_D", results.get(2).getValue().toStringUtf8(), "Expected third value value_for_D");
        assertEquals("value_for_E", results.get(3).getValue().toStringUtf8(), "Expected fourth value value_for_E");
    }
}