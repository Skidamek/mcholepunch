pluginManagement {
	repositories {
		mavenLocal()
		mavenCentral()
		gradlePluginPortal()
	}
}

dependencyResolutionManagement {
	repositories {
		mavenCentral()
	}
}

plugins {
	id("org.gradle.toolchains.foojay-resolver-convention") version providers.gradleProperty("pluginFoojayVersion").get()
}

rootProject.name = "mcholepunch"
include(":core", ":server-netty")
