/*
=====================================================================
Stored procedure: Load Bronze Layer (Source -> Bronze)
=====================================================================

Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performds the folowing actions:
- Truncate the bronze tables before loading data.
- Uses the 'COPY' command to load data from csv files to bronze tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
CALL bronze.load_bronze()

=====================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  start_time timestamp;
end_time timestamp;
start_time_whole_load timestamp;
end_time_whole_load timestamp;
BEGIN
 
start_time_whole_load = clock_timestamp();
RAISE NOTICE '===================================================';
RAISE NOTICE 'Loading Bronze Layer';
RAISE NOTICE '===================================================';
RAISE NOTICE '***************************************************';
RAISE NOTICE 'Loading ERP Tables';
RAISE NOTICE '***************************************************';

start_time = clock_timestamp();
RAISE NOTICE '>>------------------------';
RAISE NOTICE 'Truncating table: erp_clientes';
TRUNCATE TABLE bronze.erp_clientes;

RAISE NOTICE 'Inserting data into: erp_clientes';
COPY bronze.erp_clientes
FROM '/var/lib/postgresql/imports/clientes_full.csv'
WITH (
	FORMAT csv, 
	HEADER true, 
	DELIMITER ',', 
	QUOTE '''',
	ENCODING 'LATIN1'
);
end_time = clock_timestamp();

RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

start_time = clock_timestamp();
RAISE NOTICE '>>------------------------';
RAISE NOTICE 'Truncating table: erp_divisiones';
TRUNCATE TABLE bronze.erp_divisiones;

RAISE NOTICE 'Inserting data into: erp_divisiones';
COPY bronze.erp_divisiones
FROM '/var/lib/postgresql/imports/divisiones_full.csv'
WITH (
	FORMAT csv, 
	HEADER true, 
	DELIMITER ',', 
	QUOTE '''',
	ENCODING 'LATIN1'
);
end_time = clock_timestamp();

RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

start_time = clock_timestamp();
RAISE NOTICE '>>------------------------';
RAISE NOTICE 'Truncating table: erp_docclientesm';
TRUNCATE TABLE bronze.erp_docclientesm;

RAISE NOTICE 'Inserting data into: erp_docclientesm';
COPY bronze.erp_docclientesm
FROM '/var/lib/postgresql/imports/docclientesm_full.csv'
WITH (
	FORMAT csv, 
	HEADER true, 
	DELIMITER ',', 
	QUOTE '''',
	ENCODING 'LATIN1'
);
end_time = clock_timestamp();

RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

start_time = clock_timestamp();
RAISE NOTICE '>>------------------------';
RAISE NOTICE 'Truncating table: erp_docclientesd';
TRUNCATE TABLE bronze.erp_docclientesd;

RAISE NOTICE 'Inserting data into: erp_docclientesd';
COPY bronze.erp_docclientesd
FROM '/var/lib/postgresql/imports/docclientesd_full.csv'
WITH (
	FORMAT csv, 
	HEADER true, 
	DELIMITER ',', 
	QUOTE '''',
	ENCODING 'LATIN1'
);
end_time = clock_timestamp();

RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

start_time = clock_timestamp();
RAISE NOTICE '>>------------------------';
RAISE NOTICE 'Truncating table: erp_empresas';
TRUNCATE TABLE bronze.erp_empresas;

RAISE NOTICE 'Inserting data into: erp_empresas';
COPY bronze.erp_empresas
FROM '/var/lib/postgresql/imports/empresas_full.csv'
WITH (
	FORMAT csv, 
	HEADER true, 
	DELIMITER ',', 
	QUOTE '''',
	ENCODING 'LATIN1'
);
end_time = clock_timestamp();

RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

start_time = clock_timestamp();
RAISE NOTICE '>>------------------------';
RAISE NOTICE 'Truncating table: erp_gestores';
TRUNCATE TABLE bronze.erp_gestores;

RAISE NOTICE 'Inserting data into: erp_gestores';
COPY bronze.erp_gestores
FROM '/var/lib/postgresql/imports/gestores_full.csv'
WITH (
	FORMAT csv, 
	HEADER true, 
	DELIMITER ',', 
	QUOTE '''',
	ENCODING 'LATIN1'
);
end_time = clock_timestamp();

RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

start_time = clock_timestamp();
RAISE NOTICE '>>------------------------';
RAISE NOTICE 'Truncating table: erp_productos';
TRUNCATE TABLE bronze.erp_productos;

RAISE NOTICE 'Inserting data into: erp_productos';
COPY bronze.erp_productos
FROM '/var/lib/postgresql/imports/productos_full.csv'
WITH (
	FORMAT csv, 
	HEADER true, 
	DELIMITER ',', 
	QUOTE '''',
	ENCODING 'LATIN1'
);
end_time = clock_timestamp();

RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

start_time = clock_timestamp();
RAISE NOTICE '>>------------------------';
RAISE NOTICE 'Truncating table: erp_inventario';
TRUNCATE TABLE bronze.erp_inventario;

RAISE NOTICE 'Inserting data into: erp_inventario';
COPY bronze.erp_inventario
FROM '/var/lib/postgresql/imports/inventario_full.csv'
WITH (
	FORMAT csv, 
	HEADER true, 
	DELIMITER ',', 
	QUOTE '''',
	ENCODING 'LATIN1'
);
end_time = clock_timestamp();


RAISE NOTICE 'Load duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

end_time_whole_load = clock_timestamp();
RAISE NOTICE '===================================================';
RAISE NOTICE 'Loading Bronze Layer is completed';
RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time_whole_load - start_time_whole_load));
RAISE NOTICE '===================================================';

EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Error during data loading: %', SQLERRM;

END;
$$;
