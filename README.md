# Apache Flink Testing Pyramid

## Introduction

This project illustrates how to test Apache Flink applications on different levels of the testing pyramid. Additional content on this topic can be also found in the corresponding part of the [Apache Flink documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/testing.html).

The projects currently contains examples for

* unit tests of [Stateless UDFs](../master/src/test/java/com/github/knaufk/testing/java/udfs/FlattenFunctionTest.java)
* unit tests of [Stateful UDFs](../master/src/test/java/com/github/knaufk/testing/java/udfs/EvenTimeWindowCounterHarnessTest.java) incl. [WatermarkAssigners](../master/src/test/java/com/github/knaufk/testing/java/udfs/WatermarkAssignerHarnessTest.java)
* integration tests for complete [Flink Jobs](../master/src/test/java/com/github/knaufk/testing/java/StreamingJobIntegrationTest.java)

## Build

Simply checkout the repository and run the the `gradle` build with the included gradle wrapper:

```
git clone git@github.com:knaufk/flink-testing-pyramide.git
cd flink-testing-pyramide
./gradlew build
```

## License

Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0

Apache Flink, Flink®, Apache®, the squirrel logo, and the Apache feather logo are either
registered trademarks or trademarks of The Apache Software Foundation.


