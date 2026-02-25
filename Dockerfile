###############
### STAGE 1: Build with Gradle
###############
FROM azul/zulu-openjdk-alpine:21 AS builder

RUN apk add --no-cache bash dos2unix git

WORKDIR /app

COPY . .

ARG FINERACT_VERSION=1.12.1

RUN dos2unix gradlew && chmod +x gradlew && \
    rm -f .git && git init && \
    git config user.email "build@docker" && git config user.name "build" && \
    git add . && git commit -m "build" && \
    git tag ${FINERACT_VERSION} && \
    git branch -m release/${FINERACT_VERSION} && \
    ./gradlew bootJar -x test -x cucumber --no-daemon -Dorg.gradle.jvmargs="-Xmx2g"

###############
### STAGE 2: Run
###############
FROM azul/zulu-openjdk-alpine:21

WORKDIR /app

COPY --from=builder /app/fineract-provider/build/libs/fineract-provider-*.jar /app/fineract-provider.jar

EXPOSE 8080 8443

ENTRYPOINT ["java", \
  "-Duser.home=/tmp", \
  "-Dfile.encoding=UTF-8", \
  "-Duser.timezone=UTC", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-jar", "/app/fineract-provider.jar"]
