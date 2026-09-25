// Bill of materials for the Athena gateway client. Besides aligning versions,
// it makes this a multi-artifact build, which is what makes JitPack keep the
// `athena-gateway-client` artifact name instead of renaming it to the repo name.
plugins {
    `java-platform`
    `maven-publish`
}

group = rootProject.group
version = rootProject.version

dependencies {
    constraints { api(project(":")) }
}

publishing {
    publications {
        create<MavenPublication>("bom") {
            artifactId = "athena-gateway-bom"
            from(components["javaPlatform"])
            pom {
                name.set("Athena Gateway Client BOM")
                description.set("Version alignment for the Athena gateway client.")
                url.set("https://github.com/Gekkotron/Athena-HttpMqttGateway")
                licenses { license { name.set("MIT"); url.set("https://opensource.org/licenses/MIT") } }
                developers { developer { id.set("Gekkotron"); name.set("Gekkotron") } }
                scm { url.set("https://github.com/Gekkotron/Athena-HttpMqttGateway") }
            }
        }
    }
}
