-- vim:ft=plsql
\echo Use "CREATE EXTENSION auditoria" to load this file. \quit

CREATE SCHEMA auditoria;

CREATE TABLE IF NOT EXISTS auditoria.registro_auditoria (
    id SERIAL PRIMARY KEY,
    fecha_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    usuario TEXT,
    ip_cliente TEXT,
    tabla_afectada TEXT,
    tipo_operacion TEXT,
    datos_anteriores JSONB,
    datos_nuevos JSONB
);

CREATE OR REPLACE FUNCTION auditoria.registrar_actividad(tabla_afectada TEXT, tipo_operacion TEXT, datos_anteriores JSONB, datos_nuevos JSONB)
RETURNS VOID AS $$
DECLARE
    ip_cliente TEXT;
    cliente_name TEXT;
BEGIN
    -- SELECT ip_cliente = P.client_addr::TEXT, cliente_name = P.usename FROM pg_stat_activity as P WHERE pid = pg_backend_pid() LIMIT 1;
    SELECT client_addr INTO ip_cliente FROM pg_stat_activity WHERE pid = pg_backend_pid() LIMIT 1;
    SELECT usename INTO cliente_name FROM pg_stat_activity WHERE pid = pg_backend_pid() LIMIT 1;

    --SELECT client_addr INTO ip_cliente FROM pg_stat_activity WHERE pid = pg_backend_pid() LIMIT 1;
    INSERT INTO auditoria.registro_auditoria (usuario, ip_cliente, tabla_afectada, tipo_operacion, datos_anteriores, datos_nuevos)
    VALUES (cliente_name, ip_cliente, tabla_afectada, tipo_operacion, datos_anteriores, datos_nuevos);
END;
$$ LANGUAGE plpgsql
SECURITY DEFINER;

CREATE OR REPLACE FUNCTION auditoria.trig_registrar_actividad()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'INSERT') THEN
        PERFORM auditoria.registrar_actividad(TG_TABLE_NAME, TG_OP, '{}'::JSONB, row_to_json(NEW)::JSONB);
    ELSIF (TG_OP = 'UPDATE') THEN
        PERFORM auditoria.registrar_actividad(TG_TABLE_NAME, TG_OP, row_to_json(OLD)::JSONB, row_to_json(NEW)::JSONB);
    ELSIF (TG_OP = 'DELETE') THEN
        PERFORM auditoria.registrar_actividad(TG_TABLE_NAME, TG_OP, row_to_json(OLD)::JSONB, '{}'::JSONB);
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER IMMUTABLE;

CREATE OR REPLACE PROCEDURE auditoria.registrar_auditores(nombre_schema TEXT, nombre_tabla TEXT)
AS $$
BEGIN
    EXECUTE format('CREATE OR REPLACE TRIGGER registro_delete AFTER DELETE ON %I.%I FOR EACH ROW EXECUTE FUNCTION auditoria.trig_registrar_actividad()', nombre_schema, nombre_tabla);
    EXECUTE format('CREATE OR REPLACE TRIGGER registro_insert AFTER INSERT ON %I.%I FOR EACH ROW EXECUTE FUNCTION auditoria.trig_registrar_actividad()', nombre_schema, nombre_tabla);
    EXECUTE format('CREATE OR REPLACE TRIGGER registro_update AFTER UPDATE ON %I.%I FOR EACH ROW EXECUTE FUNCTION auditoria.trig_registrar_actividad()', nombre_schema, nombre_tabla);
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE PROCEDURE auditoria.registrar_en_todo_lado()
AS $$
DECLARE
    nombre_schema TEXT; nombre_tabla TEXT;
BEGIN
    FOR nombre_schema IN (SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('auditoria', 'pg_catalog', 'pg_toast', 'information_schema')) LOOP
        FOR nombre_tabla IN (SELECT table_name FROM information_schema.tables WHERE table_schema = nombre_schema and table_type != 'VIEW') LOOP
            call auditoria.registrar_auditores(nombre_schema, nombre_tabla);
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE OR REPLACE FUNCTION auditoria.registrar_en_tablas_nuevas()
RETURNS event_trigger AS $$
DECLARE
    nombre_schema TEXT;
    nombre_tabla TEXT;
    obj RECORD;
BEGIN
    FOR obj IN SELECT * FROM pg_event_trigger_ddl_commands() WHERE command_tag = 'CREATE TABLE' LOOP
        SELECT split_part(obj.object_identity, '.', 1) INTO nombre_schema;
        SELECT split_part(obj.object_identity, '.', 2) INTO nombre_tabla;
        call auditoria.registrar_auditores(nombre_schema, nombre_tabla);
    END LOOP;
END; 
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE EVENT TRIGGER
on_create_table ON ddl_command_end
WHEN TAG IN ('CREATE TABLE')
EXECUTE PROCEDURE auditoria.registrar_en_tablas_nuevas();

call auditoria.registrar_en_todo_lado();
