# dbt Mesh Demo (dbt Core)

Dagster project demonstrating orchestration of two dbt Core projects that use **dbt mesh** for cross-project references. A custom `DbtMeshComponent` handles cross-project lineage by creating external assets for models referenced via dbt mesh.

## Architecture

```
dbt_projects/
  silver_project/   Bronze -> Silver (seeds, staging views, enriched tables)
  gold_project/     Silver -> Gold (customer_360, revenue_summary, product_performance)
                    References silver models via {{ ref('silver_project', 'model') }}

src/dbt_mesh_demo/
  components/
    dbt_mesh_component.py   Custom DbtMeshComponent (subclasses DbtProjectComponent)
  defs/
    silver_transform/       Component instance for the silver dbt project
    gold_transform/         Component instance for the gold dbt project
```

### Asset Groups

| Group    | Models                                            |
|----------|---------------------------------------------------|
| **raw**  | Seeds: raw_customers, raw_orders, raw_products, raw_order_items |
| **bronze** | Staging views: stg_customers, stg_orders, stg_products, stg_order_items |
| **silver** | Enriched tables: customers, orders, order_items |
| **gold** | Analytics: customer_360, revenue_summary, product_performance |

### DbtMeshComponent Features

- **`external_packages`**: Creates external asset specs for public models from other dbt projects, preserving cross-project lineage without duplicating assets.
- **`group_overrides`**: Maps dbt resource types or fqn path segments to Dagster groups via YAML, even when the dbt project doesn't define groups.

## Getting Started

### Install dependencies

```bash
uv sync
```

### Build the dbt projects

The dbt projects use DuckDB and include seed data, so they work out of the box:

```bash
# Build silver project (seeds + models + tests)
cd dbt_projects/silver_project
uv run dbt build --profiles-dir .

# Build gold project (depends on silver)
cd ../gold_project
uv run dbt deps --profiles-dir .
uv run dbt build --profiles-dir .
```

### Run Dagster

```bash
uv run dg dev
```

Open http://localhost:3000 to see the full asset graph with cross-project lineage.

### Validate

```bash
uv run dg check defs
uv run dg list defs
```

## Customization

### Group Overrides

Group assignment is configured in `defs.yaml` via `group_overrides`:

```yaml
attributes:
  group_overrides:
    seed: raw          # All seeds -> "raw" group
    staging: bronze    # Models in staging/ -> "bronze" group
    silver: silver     # Models in silver/ -> "silver" group
```

Matches are checked by resource_type first, then by fqn path segment. Falls back to the dbt-defined group if no override matches.

### External Packages

The gold project uses `external_packages` to create external assets for silver models it references:

```yaml
attributes:
  exclude: "package:silver_project"
  external_packages:
    silver_project:
      key_prefix: ["silver_project"]
      group_name: "silver"
```

Only models with `access: public` in the upstream dbt project are exposed as external assets.
