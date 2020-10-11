# Jupiter

![Maven Central](https://img.shields.io/maven-central/v/com.computablefacts/jupiter)
[![Build Status](https://travis-ci.com/computablefacts/jupiter.svg?branch=master)](https://travis-ci.com/computablefacts/jupiter)
[![codecov](https://codecov.io/gh/computablefacts/jupiter/branch/master/graph/badge.svg)](https://codecov.io/gh/computablefacts/jupiter)

This project implements 3 data stores on top of [Apache Accumulo](https://accumulo.apache.org/) : 
one for [blobs](/src/com/computablefacts/jupiter/storage/blobstore), one for [terms](/src/com/computablefacts/jupiter/storage/termstore) 
and facts (a *fact* is a predicate expression that makes a declarative statement 
about a problem domain) and one for [JSON objects](/src/com/computablefacts/jupiter/storage/datastore). 

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
                <url>https://oss.sonatype.org/content/repositories/snapshots</url>
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
