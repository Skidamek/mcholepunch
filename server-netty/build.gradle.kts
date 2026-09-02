plugins {
	`java-library`
}

base {
	archivesName = "mcholepunch-server-netty"
}

version = project.property("mod_version") as String

dependencies {
	api(project(":core"))
	api(libs.netty.all)
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
