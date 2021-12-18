# Jupiter

![Maven Central](https://img.shields.io/maven-central/v/com.computablefacts/jupiter)
[![Build Status](https://travis-ci.com/computablefacts/jupiter.svg?branch=master)](https://travis-ci.com/computablefacts/jupiter)
[![codecov](https://codecov.io/gh/computablefacts/jupiter/branch/master/graph/badge.svg)](https://codecov.io/gh/computablefacts/jupiter)

This project implements 3 data stores on top of [Apache Accumulo](https://accumulo.apache.org/) : 
one for [blobs](#blobstore), one for [terms](#termstore) 
and facts (a *fact* is a predicate expression that makes a declarative statement 
about a problem domain) and one for [JSON objects](#datastore). 

These data stores **are not** meant to be efficients but are intended to be easy to use.

## Adding Jupiter to your build

Jupiter's Maven group ID is `com.computablefacts` and its artifact ID is `jupiter`.

To add a dependency on Jupiter using Maven, use the following:

```xml
<dependency>
  <groupId>com.computablefacts</groupId>
  <artifactId>jupiter</artifactId>
  <version>0.x</version>
</dependency>
```

## Snapshots 

Snapshots of Jupiter built from the `master` branch are available through Sonatype 
using the following dependency:

```xml
<dependency>
  <groupId>com.computablefacts</groupId>
  <artifactId>jupiter</artifactId>
  <version>0.x-SNAPSHOT</version>
</dependency>
```

In order to be able to download snapshots from Sonatype add the following profile 
to your project `pom.xml`:

```xml
 <profiles>
    <profile>
        <id>allow-snapshots</id>
        <activation><activeByDefault>true</activeByDefault></activation>
        <repositories>
            <repository>
                <id>snapshots-repo</id>
                <url>https://s01.oss.sonatype.org/content/repositories/snapshots</url>
                <releases><enabled>false</enabled></releases>
                <snapshots><enabled>true</enabled></snapshots>
            </repository>
        </repositories>
    </profile>
</profiles>
```

## Publishing a new version

Deploy a release to Maven Central with these commands:

```bash
$ git tag <version_number>
$ git push origin <version_number>
```

To update and publish the next SNAPSHOT version, just change and push the version:

```bash
$ mvn versions:set -DnewVersion=<version_number>-SNAPSHOT
$ git commit -am "Update to version <version_number>-SNAPSHOT"
$ git push origin master
```

## Points of interest

### BlobStore

The [BlobStore](/src/com/computablefacts/jupiter/storage/blobstore) API allows 
your application to persist data objects. Methods are available to write and read
opaque Strings, JSON and Files.

```java
Configurations configurations = ...;
BlobStore blobStore = new BlobStore(configurations, "blobs" /* table name */);

// Write blobs
Set<String> noBlobSpecificVizLabels = Sets.newHashSet();

try (BatchWriter writer = blobStore.writer()) {
    
    String str = ...;
    blobStore.putString(writer, "my_strings", UUID.randomUUID().toString(), noBlobSpecificVizLabels, str);

    Map<String, Object> json = ...;
    blobStore.putJson(writer, "my_jsons", UUID.randomUUID().toString(), noBlobSpecificVizLabels, json);

    File file = ...;
    blobStore.putFile(writer, "my_files", UUID.randomUUID().toString(), noBlobSpecificVizLabels, file);
}

// Read blobs
// For convenience, <dataset>_RAW_DATA authorizations are automatically added to each blob
Authorizations auths = new Authorizations("MY_STRINGS_RAW_DATA", "MY_JSONS_RAW_DATA", "MY_FILES_RAW_DATA");

blobStore.strings(auths, "my_strings", null, null).forEachRemaining(blob -> ...);
blobStore.jsons(auths, "my_jsons", null, null).forEachRemaining(blob -> ...);
blobStore.files(auths, "my_files", null, null).forEachRemaining(blob -> ...);
```

Note that it is possible to filter-out JSON fields at the tserver level before 
returning the JSON object to the client.

```java
Map<String, Object> json = new HashMap<>();
json.put("first_name", "john");
json.put("last_name", "doe");
json.put("email", "john.doe@gmail.com");
json.put("password", "&N?8LXtT7&f4@nH$");

try (BatchWriter writer = blobStore.writer()) {
    blobStore.putJson(writer, "my_jsons", UUID.randomUUID().toString(), Sets.newHashSet(), json);
}

Set<String> fieldsToKeep = Sets.newHashSet("first_name", "last_name", "email");
Optional<Value> blob = blobStore.jsons(scanner, "my_jsons", null, fieldsToKeep).first();

json.remove("password");
Assert.assertEquals(json, Codecs.asObject(blob.get().toString()));
```

### TermStore

The [TermStore](/src/com/computablefacts/jupiter/storage/termstore) API allows
your application to persist buckets of key-value pairs. Numbers and dates are 
automatically lexicoded to maintain their native Java sort order.

```java
Configurations configurations = ...;
TermStore termStore = new TermStore(configurations, "terms" /* table name */);

Map<String, Object> bucket = new HashMap<>();
bucket.put("first_name", "john");
bucket.put("last_name", "doe");
bucket.put("age", 37);
bucket.put("last_seen", new Date());

String dataset = "my_buckets";
String bucketId = UUID.randomUUID().toString();
Set<String> bucketSpecificLabels = Sets.newHashSet("MY_BUCKETS_RAW_DATA");

// Write terms
try (BatchWriter writer = termStore.writer()) {
    
    bucket.entrySet().forEach(keyValuePair -> {
        
        String field = keyValuePair.getKey();
        Object value = keyValuePair.getValue();
        Set<String> fieldSpecificLabels = Sets.newHashSet();
        
        boolean isOk = termStore.put(writer, dataset, bucketId, key, value, 1, bucketSpecificLabels, fieldSpecificLabels);
    });
}

Authorizations auths = new Authorizations("MY_BUCKETS_RAW_DATA");

/* Get the number of distinct buckets containing a given term */
        
// Wildcard query
termStore.termCardinalityEstimationForBuckets(scanner, dataset, "joh*").forEachRemaining(estimation -> ...);

// Range query
termStore.termCardinalityEstimationForBuckets(scanner, dataset, null, 30, 40).forEachRemaining(estimation -> ...);

/* Get buckets ids containing a given term */

// Wildcard query
termStore.bucketsIds(scanner, dataset, "joh*").forEachRemaining(term -> ...);
        
// Range query    
termStore.bucketsIds(scanner, dataset, null, 30, 40, null).forEachRemaining(term -> ...);
```