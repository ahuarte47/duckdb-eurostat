# DuckDB EUROSTAT Extension Function Reference

## Function Index
**[Table Functions](#table-functions)**

| Function | Summary |
| --- | --- |
| [`EUROSTAT_Dataflows`](#eurostat_dataflows) | Returns info of the dataflows provided by EUROSTAT Providers. |
| [`EUROSTAT_Endpoints`](#eurostat_endpoints) | Returns the list of supported EUROSTAT API Endpoints. |

----

## Table Functions

### EUROSTAT_Dataflows

#### Signature

```sql
EUROSTAT_Dataflows (providers VARCHAR[], dataflows VARCHAR[], language VARCHAR)
```

#### Description


		Returns info of the dataflows provided by EUROSTAT Providers.


#### Example

```sql

		SELECT * FROM EUROSTAT_Dataflows();
		SELECT * FROM EUROSTAT_Dataflows(providers = ['ESTAT','ECFIN'], language = 'en');

		--- You can also filter by specific datasets:

		SELECT
			provider_id,
			dataflow_id,
			class,
			version,
			label
		FROM
			EUROSTAT_Dataflows(providers = ['ESTAT'], dataflows = ['DEMO_R_D2JAN'], language = 'de')
		;

		┌─────────────┬──────────────┬─────────┬─────────┬───────────────────────────────────────────────────────────────────┐
		│ provider_id │  dataflow_id │  class  │ version │                               label                               │
		│   varchar   │   varchar    │ varchar │ varchar │                              varchar                              │
		├─────────────┼──────────────┼─────────┼─────────┼───────────────────────────────────────────────────────────────────┤
		│ ESTAT       │ DEMO_R_D2JAN │ dataset │ 1.0     │ Bevölkerung am 1. Januar nach Alter, Geschlecht und NUTS-2-Region │
		└─────────────┴──────────────┴─────────┴─────────┴───────────────────────────────────────────────────────────────────┘

```

----

### EUROSTAT_Endpoints

#### Signature

```sql
EUROSTAT_Endpoints ()
```

#### Description


		Returns the list of supported EUROSTAT API Endpoints.


#### Example

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

----

