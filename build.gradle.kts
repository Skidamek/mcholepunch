plugins {
	id("com.diffplug.spotless") version libs.versions.spotless.get()
}

allprojects {
	group = providers.gradleProperty("maven_group").get()
	version = providers.gradleProperty("mod_version").get()
}

subprojects {
	apply(plugin = "java-library")

	tasks.withType<JavaCompile>().configureEach {
		options.encoding = "UTF-8"
	}

	tasks.withType<Test>().configureEach {
		useJUnitPlatform()
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
