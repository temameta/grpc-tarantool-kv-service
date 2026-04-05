# gRPC Key-Value Service (Tarantool)

Разработка gRPC сервиса на Java для работы с хранилищем Tarantool.
## Быстрый старт
### Внимание!!
Перед тем, как запускать тесты или сервер, необходимо поднять *Docker*-контейнер, так как они работают с реальной БД Tarantool:
```bash
docker run -p 3301:3301 -d tarantool/tarantool
```
Внешний порт **можно поставить другой**, если 3301 занят. Менять код при этом **не обязательно** - достаточно передать **новый порт** через переменную окружения `TARANTOOL_PORT` при запуске *(см. раздел "Запуск с другими портами")*.  
Примечание: ручная настройка схемы БД **не требуется**. При старте приложение автоматически **выполняет Lua-миграции**, **создает спейс KV** и **первичные индексы** с параметром if_not_exists.

### Сборка проекта (без тестов):

```bash
mvn clean package -DskipTests
```
### Запуск интеграционных тестов:

```bash
mvn clean test
```
### Запуск gRPC сервера:

```bash
mvn compile exec:java -Dexec.mainClass="org.example.KvServer"
```
Сервер запустится на порту 9090.  
**Конфигурация (хост/порты) вынесена в переменные окружения внутри класса KvServer для удобства эксплуатации.**

### Запуск с другими портами и хостами:
По умолчанию сервер ищет Tarantool на `127.0.0.1:3301` и запускает gRPC на порту `9090`. Можно переопределить эти значения:

*Для Linux / macOS / Git Bash:*
```bash
TARANTOOL_HOST=127.0.0.1 TARANTOOL_PORT=3301 GRPC_PORT=8080 mvn compile exec:java -Dexec.mainClass="org.example.KvServer"
```
*Для Windows (PowerShell):*
```shell
$env:TARANTOOL_HOST="127.0.0.1"; $env:TARANTOOL_PORT="3301"; $env:GRPC_PORT="8080"; mvn compile exec:java -Dexec.mainClass="org.example.KvServer"
```

## Реализованное API
Контракт сервиса (src/main/proto/kv_service.proto) описывает 5 методов.

1. Put(PutRequest) returns (PutResponse)   
Сохраняет или перезаписывает значение. Корректно обрабатывает отсутствие поля value (сохраняет null в БД).
2. Get(GetRequest) returns (GetResponse)  
Возвращает значение по ключу. Если ключ не найден, возвращает gRPC ошибку со статусом NOT_FOUND. Корректно возвращает пустой value, если в БД хранится null.
3. Delete(DeleteRequest) returns (DeleteResponse)  
Удаляет запись по ключу.  
Примечание: метод возвращает success = true, если запись была успешно удалена. Если ключа в базе изначально не было, метод вернет success = false, при этом не вызывая ошибок.
4. Range(RangeRequest) returns (stream KeyValuePair)  
Возвращает поток пар ключ-значение в заданном диапазоне [key_since, key_to]. Фильтрация происходит не в сервисе, а на стороне БД через Lua-итератор box.space.KV:pairs(..., {iterator = 'GE'}). Клиенту отдаются только нужные записи, сервер не скачивает лишние данные.
5. Count(Empty) returns (CountResponse)
Возвращает общее количество записей в БД. Использован нативный метод Тарантула box.space.KV:len(). Подсчет выполняется за время O(1) без выгрузки данных на сервер.


## Стек технологий
*   **Java 21**.
*   **gRPC & Protobuf** - реализация контракта API. Использованы `optional bytes` для поддержки `null` значений.
*   **Tarantool Java SDK (v1.5.0)**. Использован `TarantoolBoxClient` для работы напрямую с БД.
*   **JUnit 5 & In-Process gRPC** - интеграционные тесты. gRPC-сервер поднимается в оперативной памяти (без занятия портов ОС), что делает тесты быстрыми и стабильными.
*   **SLF4J + Lombok** - логирование.

