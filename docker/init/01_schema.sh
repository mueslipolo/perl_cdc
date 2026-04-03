#!/bin/bash
# Run as APPUSER — gvenzl/oracle-free executes .sql as SYSDBA
sqlplus -s appuser/apppass@//localhost/FREEPDB1 <<'EOF'
WHENEVER SQLERROR EXIT SQL.SQLCODE;

CREATE TABLE departments (
    id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        VARCHAR2(100) NOT NULL,
    location    VARCHAR2(100),
    created_at  TIMESTAMP DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE employees (
    id            NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    department_id NUMBER        REFERENCES departments(id),
    first_name    VARCHAR2(50)  NOT NULL,
    last_name     VARCHAR2(50)  NOT NULL,
    email         VARCHAR2(150) UNIQUE NOT NULL,
    salary        NUMBER(12, 2),
    active        NUMBER(1)     DEFAULT 1 NOT NULL CHECK (active IN (0, 1)),
    created_at    TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    updated_at    TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL
);

EXIT;
EOF
