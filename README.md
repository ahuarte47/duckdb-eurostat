# DuckDB Eurostat Extension

## What is this?

This is an extension for DuckDB for reading data from EUROSTAT database using SQL.

[Eurostat](https://ec.europa.eu/eurostat/) is the official statistical office of the European Union, responsible for providing high-quality, comparable, and harmonised statistics on Europe’s economy, society, environment, and more.

Eurostat develops harmonised definitions, classifications and methodologies for the production of European official statistics, in cooperation with national statistical authorities. It calculates aggregate data for the European Union and eurozone, using data collected by national statistical authorities according to the harmonised standards. Eurostat makes European statistics freely available to decision‑makers and citizens via the Eurostat website, social media and other channels.

## How do I get it?

### Loading from community (TODO)

The DuckDB **EUROSTAT Extension** is available as a signed [community extension](https://duckdb.org/community_extensions/list_of_extensions).
See more details on its [DuckDB CE web page](https://duckdb.org/community_extensions/extensions/eurostat.html).

To install and load it, you can run the following SQL commands in DuckDB:

```sql
INSTALL eurostat FROM community;
LOAD eurostat;
```

### Building from source

This extension is based on the [DuckDB extension template](https://github.com/duckdb/extension-template).

## Example Usage

First, make sure to load the extension in your DuckDB session.

Then you can use the extension to read metadata and datasets from EUROSTAT database.

This is the list of available functions:

+ ### EUROSTAT_Endpoints

    Returns the list of supported EUROSTAT API Endpoints.

    ```sql
	SELECT provider_id, organization, description FROM EUROSTAT_Endpoints();

	┌─────────────┬──────────────┬──────────────────────────────────────────────────────┐
	│ provider_id │ organization │                     description                      │
	│   varchar   │   varchar    │                       varchar                        │
	├─────────────┼──────────────┼──────────────────────────────────────────────────────┤
	│ ECFIN       │ DG ECFIN     │ Economic and Financial Affairs                       │
	│ EMPL        │ DG EMPL      │ Employment, Social Affairs and Inclusion             │
	│ ESTAT       │ EUROSTAT     │ EUROSTAT database                                    │
	│ GROW        │ DG GROW      │ Internal Market, Industry, Entrepreneurship and SMEs │
	│ TAXUD       │ DG TAXUD     │ Taxation and Customs Union                           │
	└─────────────┴──────────────┴──────────────────────────────────────────────────────┘
    ```

+ ### EUROSTAT_Dataflows

    Returns info of the dataflows provided by EUROSTAT Providers.

    ```sql
	SELECT * FROM EUROSTAT_Dataflows();
	SELECT * FROM EUROSTAT_Dataflows(providers = ['ESTAT','ECFIN'], language := 'en');
	```

	You can also filter by specific dataflows:

	```sql
	SELECT
		provider_id,
		dataflow_id,
		class,
		version,
		label
	FROM
		EUROSTAT_Dataflows(providers = ['ESTAT'], dataflows = ['DEMO_R_D2JAN'], language := 'de')
	;

	┌─────────────┬──────────────┬─────────┬─────────┬───────────────────────────────────────────────────────────────────┐
	│ provider_id │  dataflow_id │  class  │ version │                               label                               │
	│   varchar   │   varchar    │ varchar │ varchar │                              varchar                              │
	├─────────────┼──────────────┼─────────┼─────────┼───────────────────────────────────────────────────────────────────┤
	│ ESTAT       │ DEMO_R_D2JAN │ dataset │ 1.0     │ Bevölkerung am 1. Januar nach Alter, Geschlecht und NUTS-2-Region │
	└─────────────┴──────────────┴─────────┴─────────┴───────────────────────────────────────────────────────────────────┘
    ```

+ ### EUROSTAT_DataStructure

    Returns information of the data structure of an EUROSTAT Dataflow.

    ```sql
	SELECT
		provider_id,
		dataflow_id,
		position,
		dimension,
		concept
	FROM
		EUROSTAT_DataStructure('ESTAT', 'DEMO_R_D2JAN', language := 'en')
	;

	┌─────────────┬──────────────┬──────────┬─────────────┬─────────────────────────────────┐
	│ provider_id │ dataflow_id  │ position │  dimension  │             concept             │
	│   varchar   │   varchar    │  int32   │   varchar   │             varchar             │
	├─────────────┼──────────────┼──────────┼─────────────┼─────────────────────────────────┤
	│ ESTAT       │ DEMO_R_D2JAN │        1 │ freq        │ Time frequency                  │
	│ ESTAT       │ DEMO_R_D2JAN │        2 │ unit        │ Unit of measure                 │
	│ ESTAT       │ DEMO_R_D2JAN │        3 │ sex         │ Sex                             │
	│ ESTAT       │ DEMO_R_D2JAN │        4 │ age         │ Age class                       │
	│ ESTAT       │ DEMO_R_D2JAN │        5 │ geo         │ Geopolitical entity (reporting) │
	│ ESTAT       │ DEMO_R_D2JAN │       -1 │ geo_level   │ NUTS classification level       │
	│ ESTAT       │ DEMO_R_D2JAN │        6 │ time_period │ Time                            │
	└─────────────┴──────────────┴──────────┴─────────────┴─────────────────────────────────┘
    ```

	`geo_level` is a dimension that is not part of the dataflow source, but it is computed based
	on the `geo` dimension values. See the function [EUROSTAT_GetGeoLevelFromGeoCode](#eurostat_getgeolevelfromgeocode) below for
	more details.

+ ### EUROSTAT_Read

    Reads the dataset of an EUROSTAT Dataflow.

    ```sql
	SELECT * FROM EUROSTAT_Read('ESTAT', 'DEMO_R_D2JAN') LIMIT 5;

	┌─────────┬─────────┬─────────┬─────────┬─────────┬───────────┬─────────────┬───────────────────┐
	│  freq   │  unit   │   sex   │   age   │   geo   │ geo_level │ TIME_PERIOD │ observation_value │
	│ varchar │ varchar │ varchar │ varchar │ varchar │  varchar  │   varchar   │      double       │
	├─────────┼─────────┼─────────┼─────────┼─────────┼───────────┼─────────────┼───────────────────┤
	│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2000        │         1526762.0 │
	│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2001        │         1535822.0 │
	│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2002        │         1532563.0 │
	│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2003        │         1526180.0 │
	│ A       │ NR      │ F       │ TOTAL   │ AL      │ country   │ 2004        │         1520481.0 │
	└─────────┴─────────┴─────────┴─────────┴─────────┴───────────┴─────────────┴───────────────────┘
    ```

+ ### EUROSTAT_GetGeoLevelFromGeoCode

	Scalar function that returns the level for a GEO code in the NUTS classification
	or if it is considered aggregates.

    ```sql
	SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE');        -- returns 'country'
	SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE1');       -- returns 'nuts1'
	SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE12');      -- returns 'nuts2'
	SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE123');     -- returns 'nuts3'
	SELECT EUROSTAT_GetGeoLevelFromGeoCode('DE_DEL1');   -- returns 'city'
	SELECT EUROSTAT_GetGeoLevelFromGeoCode('EU27_2020'); -- returns 'aggregate'
	```

	This scalar function is used by the `EUROSTAT_Read` function to add the `geo_level`
	dimension as a normal column.

	The supported levels are:
	- aggregate
	- country
	- nuts1
	- nuts2
	- nuts3
	- city

	See more details about `geo_level` [here](https://ec.europa.eu/eurostat/web/user-guides/data-browser/api-data-access/api-getting-started/api#APIGettingstartedwithstatisticsAPI-FilteringongeoLevel).


### Supported Functions and Documentation

The full list of functions and their documentation is available in the [function reference](docs/functions.md)

## How do I build it?

### Dependencies

You need a recent version of CMake (3.5) and a C++14 compatible compiler.

We also highly recommend that you install [Ninja](https://ninja-build.org) which you can select when building by setting the `GEN=ninja` environment variable.
```
git clone --recurse-submodules https://github.com/ahuarte47/duckdb-eurostat
cd duckdb-eurostat
make release
```

You can then invoke the built DuckDB (with the extension statically linked)
```
./build/release/duckdb
```

Please see the Makefile for more options, or the extension template documentation for more details.

### Running the tests

Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:

```sh
make test
```

### Installing the deployed binaries

To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL eurostat;
LOAD eurostat;
```

Enjoy!
