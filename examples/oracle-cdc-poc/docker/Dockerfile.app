# Perl test runner with Oracle Instant Client
# Build context should be the repository root.
FROM docker.io/library/perl:5.40-slim-bookworm

ARG ORA_CLIENT_URL_BASIC=https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-basic-linux.x64-23.7.0.25.01.zip
ARG ORA_CLIENT_URL_SDK=https://download.oracle.com/otn_software/linux/instantclient/2370000/instantclient-sdk-linux.x64-23.7.0.25.01.zip

ENV ORACLE_HOME=/opt/oracle/instantclient_23_7
ENV LD_LIBRARY_PATH=${ORACLE_HOME}

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl unzip libaio1 make gcc \
    && curl -fSL -o /tmp/basic.zip "$ORA_CLIENT_URL_BASIC" \
    && curl -fSL -o /tmp/sdk.zip   "$ORA_CLIENT_URL_SDK" \
    && mkdir -p /opt/oracle \
    && unzip -oq /tmp/basic.zip -d /opt/oracle \
    && unzip -oq /tmp/sdk.zip   -d /opt/oracle \
    && rm /tmp/basic.zip /tmp/sdk.zip \
    && apt-get purge -y curl unzip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps from example cpanfile (includes DBD::Oracle)
COPY examples/oracle-cdc-poc/cpanfile .
RUN cpanm --quiet --notest --installdeps .

# Plugin library and unit tests
COPY lib/ lib/
COPY t/   t/

# Example app library and integration tests
COPY examples/oracle-cdc-poc/lib/ examples/lib/
COPY examples/oracle-cdc-poc/t/   examples/t/

USER nobody
CMD ["prove", "-Ilib", "-Iexamples/lib", "-lv", "examples/t/"]
