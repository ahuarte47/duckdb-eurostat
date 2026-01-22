# DuckDB EUROSTAT Extension Function Reference

## Function Index
**[Table Functions](#table-functions)**

| Function | Summary |
| --- | --- |
| [`EUROSTAT_Endpoints`](#eurostat_endpoints) | Returns the list of supported EUROSTAT API Endpoints. |

----

## Table Functions

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

