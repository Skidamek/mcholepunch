import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication

plugins {
	id("com.diffplug.spotless") version libs.versions.spotless.get()
}

allprojects {
	group = providers.gradleProperty("maven_group").get()
	version = providers.gradleProperty("mod_version").get()
}

subprojects {
	apply(plugin = "java-library")
	apply(plugin = "maven-publish")

	extensions.configure<JavaPluginExtension> {
		withSourcesJar()
	}

	tasks.withType<JavaCompile>().configureEach {
		options.encoding = "UTF-8"
		options.compilerArgs.addAll(listOf("-Xlint:all,-serial", "-Werror"))
	}

	tasks.withType<Test>().configureEach {
		useJUnitPlatform()
	}

	extensions.configure<PublishingExtension> {
		publications {
			create<MavenPublication>("mavenJava") {
				artifactId = "${rootProject.name}-${project.name}"
				from(components["java"])
				pom {
					name.set(artifactId)
					description.set("Version-independent Java library for multiplexing an encrypted byte stream over a Minecraft server's TCP port.")
					url.set("https://github.com/Skidamek/mcholepunch")
					licenses {
						license {
							name.set("BSD-3-Clause")
							url.set("https://opensource.org/licenses/BSD-3-Clause")
						}
					}
					scm {
						url.set("https://github.com/Skidamek/mcholepunch")
						connection.set("scm:git:https://github.com/Skidamek/mcholepunch.git")
						developerConnection.set("scm:git:ssh://git@github.com:Skidamek/mcholepunch.git")
					}
				}
			}
		}
		repositories {
			maven {
				name = "mavenRepo"
				url =
					providers
						.gradleProperty("mavenRepoDir")
						.map { rootProject.file(it).toURI() }
						.orElse(
							rootProject.layout.buildDirectory
								.dir("maven-repo")
								.map { it.asFile.toURI() },
						).get()
			}
		}
	}
}

extensions.configure<com.diffplug.gradle.spotless.SpotlessExtension> {
	java {
		target("core/src/**/*.java", "server-netty/src/**/*.java")
		eclipse().configFile("config/format/eclipse-java.xml")
		importOrder("java", "javax", "org", "com", "", "pl.skidam")
		trimTrailingWhitespace()
		endWithNewline()
	}

	kotlinGradle {
		target("**/*.gradle.kts")
		targetExclude("versions/**", ".gradle/**", "**/build/**")
		ktlint()
		trimTrailingWhitespace()
		endWithNewline()
	}
}

tasks.register("formatApply") {
	dependsOn("spotlessApply")
}

tasks.register("formatCheck") {
	dependsOn("spotlessCheck")
}
