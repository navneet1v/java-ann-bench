import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter

plugins {
    id("java")
    id("application")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

val defaultJvmArgs = listOf(
        "--enable-preview",
        "-XX:+UnlockExperimentalVMOptions",
        "-XX:+EnableVectorSupport",
        "--add-modules",
        "jdk.incubator.vector",
        "--enable-native-access=ALL-UNNAMED",
        "-ea"
)

application {
    mainClass.set("com.github.kevindrosendahl.javaannbench.BenchRunner")
    applicationDefaultJvmArgs = defaultJvmArgs
}

dependencies {
    implementation("org.apache.commons:commons-csv:1.10.0")
    implementation("com.google.guava:guava:32.1.3-jre")
    implementation("software.amazon.awssdk:s3:2.21.5")
    implementation("software.amazon.awssdk:s3-transfer-manager:2.21.7")
    implementation("software.amazon.awssdk.crt:aws-crt:0.27.7")
    implementation("ch.qos.logback:logback-classic:1.4.11")
    implementation("commons-io:commons-io:2.14.0")
    implementation("info.picocli:picocli:4.7.5")
    implementation("me.tongfei:progressbar:0.10.0")
    implementation("com.github.oshi:oshi-core:6.4.3")
    implementation("org.apache.commons:commons-math3:3.6.1")
    implementation("org.slf4j:jul-to-slf4j:2.0.9")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.15.3")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.3")
    implementation("org.jline:jline:3.23.0")
    implementation("com.indeed:util-mmap:1.0.52-3042601")

    implementation(files("libs/lucene-core-10.0.0-SNAPSHOT.jar"))
    implementation(files("libs/jvector-1.0.3-SNAPSHOT.jar"))

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

tasks.withType<JavaCompile> {
    options.compilerArgs.add("--enable-preview")
}

tasks.named<JavaExec>("run") {
    val minHeap = project.findProperty("minHeapSize") as String?
    val maxHeap = project.findProperty("maxHeapSize") as String?

    val dynamicJvmArgs = mutableListOf<String>().apply {
        if (minHeap != null) add(minHeap)
        if (maxHeap != null) add(maxHeap)
    }

    jvmArgs = defaultJvmArgs + dynamicJvmArgs
}

tasks.test {
    useJUnitPlatform()
}

