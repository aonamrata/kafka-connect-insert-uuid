Kafka Connect SMT to add a random [UUID](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html)

This SMT supports inserting a UUID into the record Key or Value
Properties:

|Name| Description         |Type| Default   |Importance|
|---|---------------------|---|-----------|---|
|`uuid.field.value`| Value for operation | String | `updated` | High |

Example on how to add to your connector:
```
  "transforms" : "insertuuid",
"transforms.insertuuid.type": "com.github.pde.kafka.connect.smt.ChangeOperation$Value",
"transforms.insertuuid.uuid.field.value": "created"
```


ToDO
* ~~add support for records without schemas~~

Lots borrowed from the Apache Kafka® `InsertField` SMT



Run and create JAR file locally
```
mvn install
```

