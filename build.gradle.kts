repositories {
    mavenLocal()
    mavenCentral()
}

plugins {
    id("java")
    id("maven")
}

dependencies {
    implementation("javax.xml.bind:jaxb-api:2.3.1")
    compileOnly("com.google.code.findbugs:annotations:3.0.1u2")
    testImplementation("io.netty:netty-handler:4.1.33.Final")
    testImplementation("junit:junit:4.12")
    testImplementation("ru.yandex.qatools.embed:postgresql-embedded:2.10")
}

tasks.withType<JavaCompile> {
    options.encoding = "utf-8"
}

group = "com.github.alaisi.pgasync"
version = "0-10"