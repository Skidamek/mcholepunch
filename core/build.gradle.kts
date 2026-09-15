plugins {
	`java-library`
}

base {
	archivesName = "mcholepunch-core"
}

version = project.property("mod_version") as String

dependencies {
	compileOnly(libs.log4j.api)
	testRuntimeOnly(libs.log4j.core)
	testImplementation(platform(libs.junit.bom))
	testImplementation(libs.junit.jupiter.api)
	testRuntimeOnly(libs.junit.jupiter.engine)
	testRuntimeOnly(libs.junit.platform.launcher)
}

java {
	toolchain.languageVersion.set(JavaLanguageVersion.of(17))
}

tasks.test {
	useJUnitPlatform()
}
