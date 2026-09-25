import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    id("org.jetbrains.kotlinx.kover") version "0.8.3"
    `java-library`
    `maven-publish`
}

group = "com.github.Gekkotron.Athena-HttpMqttGateway"
if (version == "unspecified") version = "0.0.0-SNAPSHOT"

repositories { mavenCentral() }

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
    withSourcesJar()
}

kotlin {
    explicitApi()
    compilerOptions { jvmTarget.set(JvmTarget.JVM_11) }
}

// kotlinx-coroutines-test's virtual-time APIs (currentTime, advanceTimeBy) are
// @ExperimentalCoroutinesApi; opt in for test code only so SubscribeTest can use them
// without a per-usage annotation or new compiler warnings.
tasks.named<org.jetbrains.kotlin.gradle.tasks.KotlinCompile>("compileTestKotlin") {
    compilerOptions.optIn.add("kotlinx.coroutines.ExperimentalCoroutinesApi")
}

dependencies {
    api("com.squareup.okhttp3:okhttp:4.12.0")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.3")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.9.0")
}

tasks.test {
    useJUnitPlatform { excludeTags("integration") }
}

val integrationTest by tasks.registering(Test::class) {
    description = "Runs tests against a live gateway (see GatewayIntegrationTest)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform { includeTags("integration") }
    outputs.upToDateWhen { false }
}

kover {
    currentProject { instrumentation { disabledForTestTasks.add("integrationTest") } }
    reports { verify { rule { minBound(85) } } }
}
