plugins {
    id("java")
    id("org.jetbrains.kotlin.jvm") version "2.1.0"
    id("org.jetbrains.intellij.platform")
}

group = providers.gradleProperty("pluginGroup").get()
version = providers.gradleProperty("pluginVersion").get()

repositories {
    mavenCentral()
    intellijPlatform {
        defaultRepositories()
    }
}

dependencies {
    intellijPlatform {
        datagrip("2025.1.3")
        bundledPlugin("com.intellij.database")
    }
    implementation("org.xerial:sqlite-jdbc:3.47.2.0")
}

intellijPlatform {
    buildSearchableOptions = false
    publishing {
        token = providers.environmentVariable("PUBLISH_TOKEN")
    }
    pluginConfiguration {
        id = "com.github.dmlbackup"
        name = providers.gradleProperty("pluginName")
        version = providers.gradleProperty("pluginVersion")

        ideaVersion {
            sinceBuild = providers.gradleProperty("pluginSinceBuild")
            untilBuild = providers.gradleProperty("pluginUntilBuild")
        }

        description = """
            Automatically backup data before executing DELETE/UPDATE/INSERT statements in DataGrip (MySQL only).
            Provides one-click rollback from the backup history panel.
        """.trimIndent()
    }
}

tasks {
    runIde {
        jvmArgs("-Dsun.java2d.metal=false", "-Dsun.java2d.opengl=false")
    }
}

kotlin {
    jvmToolchain(21)
}
