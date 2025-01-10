Kafka Connect SMT to add a random [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)

This SMT supports inserting a UUID into the record Key or Value
Properties:

|Name|Description|Type|Default|Importance|
|---|---|---|---|---|
|`uuid.field.name`| Field name for UUID | String | `uuid` | High |

Example on how to add to your connector:
```
transforms=insertuuid
transforms.insertuuid.type=com.github.cjmatta.kafka.connect.smt.InsertUuid$Value
transforms.insertuuid.uuid.field.name="uuid"


transforms=getpayload
transforms.getpayload.type=com.github.pde.kafka.connect.smt.GetCDCPayload$Value
transforms.getpayload.key.field.name="uuid"
transforms.getpayload.skip.field.names="lastModifiedBy,createdBy"


```


ToDO
* ~~add support for records without schemas~~

Lots borrowed from the Apache Kafka® `InsertField` SMT



Run and create JAR file locally
```
mvn install
mvn verify -Dmaven.test.failure.ignore=true
```