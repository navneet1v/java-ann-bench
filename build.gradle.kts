plugins {
    id("java")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}


dependencies {
    implementation("org.apache.commons:commons-csv:1.10.0")
    implementation("com.google.guava:guava:32.1.3-jre")
    implementation("software.amazon.awssdk:s3:2.21.5")
    implementation("ch.qos.logback:logback-classic:1.4.11")
    implementation("commons-io:commons-io:2.14.0")

    testImplementation(platform("org.junit:junit-bom:5.9.1"))
    testImplementation("org.junit.jupiter:junit-jupiter")
}

tasks.test {
    useJUnitPlatform()
}