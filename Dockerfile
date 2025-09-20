# ===================================================================================
# STAGE 1: Builder
# This stage builds the application, using a persistent cache for dependencies.
# ===================================================================================
FROM eclipse-temurin:21-jdk-jammy AS builder

WORKDIR /app

# Explicitly create the .m2 directory so that Docker's mount command
# has a valid target path to mount the cache volume onto.
RUN mkdir -p /root/.m2

# 1. Copy only the files needed to resolve dependencies.
# This layer will be cached as long as pom.xml doesn't change.
COPY .mvn/ .mvn
COPY mvnw pom.xml ./

# 2. Resolve dependencies. This is the slow, memory-intensive step.
# --mount=type=secret: Securely provides the settings.xml for authentication.
# --mount=type=cache: Persists the downloaded JARs in a cache volume between builds,
# making subsequent builds much faster.
RUN --mount=type=secret,id=maven-settings,target=/root/.m2/settings.xml \
    --mount=type=cache,target=/root/.m2 \
    ./mvnw dependency:resolve --global-settings /root/.m2/settings.xml

# 3. Copy the application source code.
# This layer will only be rebuilt if your Java code changes.
COPY src ./src

# 4. Build the application JAR.
# This will be very fast on subsequent runs because all dependencies are already in the cache.
RUN --mount=type=secret,id=maven-settings,target=/root/.m2/settings.xml \
    --mount=type=cache,target=/root/.m2 \
    ./mvnw package -DskipTests --global-settings /root/.m2/settings.xml


# ===================================================================================
# STAGE 2: Final Image
# This stage creates the final, minimal image for production.
# ===================================================================================
FROM eclipse-temurin:21-jre-jammy

WORKDIR /app

# Copy only the final application JAR from the builder stage.
COPY --from=builder /app/target/*.jar app.jar

# Set the entrypoint to run the application.
ENTRYPOINT ["java", "-jar", "app.jar"]