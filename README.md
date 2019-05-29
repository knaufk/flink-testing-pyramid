# Apache Flink Testing Pyramid

## Introduction

This project illustrates how to test Apache Flink applications on different levels of the testing pyramid. Additional content on this topic can be also found in the corresponding part of the [Apache Flink documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.9/dev/stream/testing.html).

The projects currently contains examples for

* unit tests of [Stateless UDFs](../master/src/test/java/com/github/knaufk/testing/java/udfs/FlattenFunctionTest.java)
* unit tests of [Stateful UDFs](../master/src/test/java/com/github/knaufk/testing/java/udfs/EvenTimeWindowCounterHarnessTest.java) incl. [WatermarkAssigners](../master/src/test/java/com/github/knaufk/testing/java/udfs/WatermarkAssignerHarnessTest.java)
* integration tests for complete [Flink Jobs](../master/src/test/java/com/github/knaufk/testing/java/StreamingJobIntegrationTest.java)

### Build

Simply checkout the repository and run the the `gradle` build with the included gradle wrapper:

```
git clone git@github.com:knaufk/flink-testing-pyramide.git
cd flink-testing-pyramide
./gradlew build
```

