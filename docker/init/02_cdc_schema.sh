#!/bin/bash
# Run as APPUSER — gvenzl/oracle-free executes .sql as SYSDBA
sqlplus -s appuser/apppass@//localhost/FREEPDB1 <<'EOF'
WHENEVER SQLERROR EXIT SQL.SQLCODE;

CREATE TABLE cdc_events (
    event_id        NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_time      TIMESTAMP    DEFAULT SYSTIMESTAMP NOT NULL,
    table_name      VARCHAR2(128) NOT NULL,
    operation       VARCHAR2(6)   NOT NULL
                        CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE')),
    old_data        CLOB,
    new_data        CLOB,
    session_user    VARCHAR2(128) DEFAULT SYS_CONTEXT('USERENV', 'SESSION_USER'),
    transaction_id  VARCHAR2(64)
);

CREATE INDEX cdc_events_table_op_idx
    ON cdc_events (table_name, operation, event_id);

EXIT;
EOF
