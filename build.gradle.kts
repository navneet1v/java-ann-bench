plugins {
    id("java")
    id("application")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

application {
    mainClass.set("com.github.kevindrosendahl.javaannbench.BenchRunner")
    applicationDefaultJvmArgs = listOf(
            "-Xmx4g",
            "--enable-preview",
            "-XX:+UnlockExperimentalVMOptions",
            "-XX:+EnableVectorSupport",
            "--add-modules",
            "jdk.incubator.vector"
    )
}

dependencies {
    implementation("org.apache.commons:commons-csv:1.10.0")
    implementation("com.google.guava:guava:32.1.3-jre")
    implementation("software.amazon.awssdk:s3:2.21.5")
    implementation("ch.qos.logback:logback-classic:1.4.11")
    implementation("commons-io:commons-io:2.14.0")
    implementation(files("libs/lucene-core-10.0.0-SNAPSHOT.jar"))
    implementation("info.picocli:picocli:4.7.5")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

tasks.test {
    useJUnitPlatform()
}