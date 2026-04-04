package org.example.kv;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.tarantool.client.box.TarantoolBoxClient;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.List;

@RequiredArgsConstructor
public class KvServiceImpl extends KvServiceGrpc.KvServiceImplBase {
    private final TarantoolBoxClient tarantoolBoxClient;
    private final String spaceName;

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        try {
            String key = request.getKey();
            byte[] value = request.hasValue() ? request.getValue().toByteArray() : null;
            List<?> tuple = Arrays.asList(key, value);
            var puttedTuple = tarantoolBoxClient.space(spaceName).replace(tuple).join();
            boolean isPutted = puttedTuple != null && !puttedTuple.get().isEmpty();
            PutResponse success = PutResponse.newBuilder().setSuccess(isPutted).build();
            responseObserver.onNext(success);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        try {
            String key = request.getKey();
            var tarantoolResponse = tarantoolBoxClient.space(spaceName).select(List.of(key)).join();
            if (tarantoolResponse.get().isEmpty()) responseObserver.onError(Status.NOT_FOUND.asException());
            else {
                var value = tarantoolResponse.get().getFirst().get().get(1);
                GetResponse.Builder response = GetResponse.newBuilder();
                if (value != null) response.setValue(ByteString.copyFrom((byte[]) value));
                responseObserver.onNext(response.build());
                responseObserver.onCompleted();
            }
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        try {
            String key = request.getKey();
            var deletedTuple = tarantoolBoxClient.space(spaceName).delete(List.of(key)).join();
            boolean isDeleted = deletedTuple != null && !deletedTuple.get().isEmpty();
            DeleteResponse response = DeleteResponse.newBuilder().setSuccess(isDeleted).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void range(RangeRequest request, StreamObserver<KeyValuePair> responseObserver) {
        try {
            String key_since = request.getKeySince();
            String key_to = request.getKeyTo();
            String luaScript = String.format("""
                        local key_since, key_to = ...
                        local result = {}
                        for _, tuple in box.space.%s:pairs({key_since}, {iterator = 'GE'}) do
                            if tuple[1] > key_to then
                                break
                            end
                            table.insert(result, tuple)
                        end
                        return result
                    """, spaceName);

            var tarantoolResponse = tarantoolBoxClient.eval(luaScript, Arrays.asList(key_since, key_to)).join();
            List<?> tuples = (List<?>) tarantoolResponse.get().getFirst();
            for (Object obj : tuples) {
                List<?> tuple = (List<?>) obj;
                String key = (String) tuple.getFirst();
                Object value = tuple.get(1);
                KeyValuePair.Builder pair = KeyValuePair.newBuilder().setKey(key);
                if (value != null) pair.setValue(ByteString.copyFrom((byte[]) value));
                responseObserver.onNext(pair.build());
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void count(Empty request, StreamObserver<CountResponse> responseObserver) {
        try {
            var tarantoolResponse = tarantoolBoxClient.eval("return box.space." + spaceName + ":len()").join();
            long count = ((Number) tarantoolResponse.get().getFirst()).longValue();
            CountResponse response = CountResponse.newBuilder().setCount(count).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }
}
