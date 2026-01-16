-- Limpiamos para empezar de cero
DROP TABLE IF EXISTS fct_transacciones CASCADE;
DROP TABLE IF EXISTS dim_clientes CASCADE;
DROP TABLE IF EXISTS dim_distribuidores CASCADE;
DROP TABLE IF EXISTS dim_sedes CASCADE;
DROP TABLE IF EXISTS dim_tipo_transaccion CASCADE;

-- Catalogo de Sedes (Viene del Excel)
CREATE TABLE dim_sedes (
    id_sede INT PRIMARY KEY,
    nombre_sede VARCHAR(50)
);

-- Catalogo de Tipos de Transaccion (Viene del Excel)
CREATE TABLE dim_tipo_transaccion (
    id_tipo_trx INT PRIMARY KEY,
    descripcion_tipo VARCHAR(50)
);

-- Tabla de Distribuidores (Esta sale del JSON)
CREATE TABLE dim_distribuidores (
    id_distribuidor INT PRIMARY KEY,
    nombre_distribuidor VARCHAR(100),
    telefono BIGINT,
    categoria VARCHAR(50)
);

-- Tabla de Clientes
CREATE TABLE dim_clientes (
    id_cliente INT PRIMARY KEY,
    fecha_afiliacion DATE,
    fecha_primera_trx DATE,
    id_distribuidor INT REFERENCES dim_distribuidores(id_distribuidor)
);

-- Tabla de Hechos: Transacciones (Facts)
CREATE TABLE fct_transacciones (
    id_trx INT PRIMARY KEY,
    id_cliente INT REFERENCES dim_clientes(id_cliente),
    id_sede INT REFERENCES dim_sedes(id_sede),
    id_tipo_trx INT REFERENCES dim_tipo_transaccion(id_tipo_trx),
    fecha_trx TIMESTAMP,
    monto DECIMAL(12, 2),
    fee DECIMAL(12, 2)
);