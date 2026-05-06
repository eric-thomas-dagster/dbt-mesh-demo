"""Custom dbt component that handles dbt mesh cross-project references.

When a dbt project includes models from an external package (via dbt mesh),
this component creates external asset specs for those models so that Dagster
can track lineage across project boundaries without duplicating assets.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Optional

import dagster as dg
from dagster.components.resolved.model import Resolver
from dagster_dbt import DagsterDbtTranslator, DbtProject, DbtProjectComponent


class _MeshTranslator(DagsterDbtTranslator):
    """Translator that supports group overrides and auto-partitioning of microbatch models."""

    def __init__(
        self,
        group_overrides: dict[str, str] | None = None,
        auto_partition_microbatch: bool = False,
        microbatch_start_date: str = "2024-01-01",
        settings: Any | None = None,
    ):
        super().__init__(settings=settings)
        self._group_overrides = group_overrides or {}
        self._auto_partition_microbatch = auto_partition_microbatch
        self._microbatch_start_date = microbatch_start_date

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        if self._group_overrides:
            resource_type = dbt_resource_props.get("resource_type", "")
            if resource_type in self._group_overrides:
                return self._group_overrides[resource_type]
            for segment in dbt_resource_props.get("fqn", []):
                if segment in self._group_overrides:
                    return self._group_overrides[segment]
        return super().get_group_name(dbt_resource_props)

    def get_partitions_def(
        self, dbt_resource_props: Mapping[str, Any]
    ) -> dg.PartitionsDefinition | None:
        if not self._auto_partition_microbatch:
            return None
        config = dbt_resource_props.get("config", {})
        if config.get("incremental_strategy") == "microbatch":
            batch_size = config.get("batch_size", "day")
            begin = config.get("begin", self._microbatch_start_date)
            # dbt may append T00:00:00 — strip to date only
            if isinstance(begin, str) and "T" in begin:
                begin = begin.split("T")[0]
            if batch_size == "day":
                return dg.DailyPartitionsDefinition(start_date=begin)
            elif batch_size == "month":
                return dg.MonthlyPartitionsDefinition(start_date=begin)
            elif batch_size == "hour":
                return dg.HourlyPartitionsDefinition(start_date=begin)
        return None


@dataclass
class DbtMeshComponent(DbtProjectComponent):
    """A DbtProjectComponent that generates external assets for cross-project dbt mesh references.

    When `external_packages` is configured, models from those packages are excluded from
    normal dbt asset creation and instead represented as external assets with proper
    lineage connections.
    """

    external_packages: Annotated[
        dict[str, dict[str, Any]],
        Resolver.default(
            description="Map of external package names to their config (key_prefix, group_name)",
        ),
    ] = field(default_factory=dict)

    group_overrides: Annotated[
        dict[str, str],
        Resolver.default(
            description=(
                "Map of dbt resource type or fqn path segment to Dagster group name. "
                "Matches are checked in order: resource_type first (e.g. 'seed'), "
                "then each fqn segment (e.g. 'staging', 'silver'). "
                "Falls back to the dbt group if set, otherwise 'default'."
            ),
        ),
    ] = field(default_factory=dict)

    auto_partition_microbatch: Annotated[
        bool,
        Resolver.default(
            description=(
                "Automatically create Dagster partition definitions for dbt models "
                "using the microbatch incremental strategy. Maps batch_size to "
                "DailyPartitionsDefinition (day), MonthlyPartitionsDefinition (month), "
                "or HourlyPartitionsDefinition (hour). Uses the model's 'begin' config "
                "as the partition start date."
            ),
        ),
    ] = False

    enable_exposures: Annotated[
        bool,
        Resolver.default(
            description=(
                "Create non-materializable external assets for dbt exposures. "
                "Exposures represent downstream consumers (dashboards, ML models, "
                "APIs) that depend on dbt models. They appear in the Dagster asset "
                "graph as leaf nodes, completing the lineage from sources through "
                "models to consumers."
            ),
        ),
    ] = False

    exposure_group: Annotated[
        str,
        Resolver.default(
            description="Dagster group name for exposure assets. Default 'exposures'.",
        ),
    ] = "exposures"

    enable_source_freshness_policies: Annotated[
        bool,
        Resolver.default(
            description=(
                "Apply Dagster FreshnessPolicy to source assets based on dbt's "
                "source freshness config (warn_after/error_after). Dagster passively "
                "evaluates freshness — no dbt execution needed. Requires an upstream "
                "sensor (Fivetran, Airbyte, or custom) to emit AssetObservation "
                "events so Dagster knows when sources were last updated."
            ),
        ),
    ] = False

    def get_cli_args(self, context: dg.AssetExecutionContext) -> list[str]:
        """Override to inject --event-time-start/end for microbatch partitions.

        When auto_partition_microbatch is enabled and the run has a partition key,
        automatically adds --event-time-start and --event-time-end to the dbt
        command so microbatch only processes the selected partition's batch window.
        """
        args = super().get_cli_args(context)

        if self.auto_partition_microbatch and context.has_partition_key:
            partition_key = context.partition_key
            # Compute end date from the partition time window
            if hasattr(context, "partition_time_window") and context.partition_time_window:
                end_date = context.partition_time_window.end.strftime("%Y-%m-%d")
            else:
                # Fallback: assume daily partition, add one day
                from datetime import datetime, timedelta

                start = datetime.strptime(partition_key, "%Y-%m-%d")
                end_date = (start + timedelta(days=1)).strftime("%Y-%m-%d")

            args.extend(["--event-time-start", partition_key, "--event-time-end", end_date])

        return args

    @cached_property
    def translator(self) -> DagsterDbtTranslator:
        from dataclasses import replace

        settings = replace(self.translation_settings, enable_code_references=False)
        if self.group_overrides or self.auto_partition_microbatch:
            return _MeshTranslator(
                group_overrides=self.group_overrides,
                auto_partition_microbatch=self.auto_partition_microbatch,
                settings=settings,
            )
        return DagsterDbtTranslator(settings)

    def build_defs_from_state(
        self, context: dg.ComponentLoadContext, state_path: Path | None
    ) -> dg.Definitions:
        base_defs = super().build_defs_from_state(context, state_path)

        needs_manifest = (
            self.external_packages
            or self.enable_exposures
            or self.enable_source_freshness_policies
        )
        if not needs_manifest:
            return base_defs

        # Get the resolved project and its manifest
        project = self._project_manager.get_project(state_path)
        manifest_path = Path(project.manifest_path)

        if not manifest_path.exists():
            return base_defs

        manifest = json.loads(manifest_path.read_text())

        additional_assets: list[dg.AssetSpec] = []

        # External assets for mesh packages
        if self.external_packages:
            additional_assets.extend(self._create_external_assets(manifest))

        # Exposure assets (downstream consumers)
        if self.enable_exposures:
            additional_assets.extend(self._create_exposure_assets(manifest))

        # Source freshness policies
        updated_assets = list(base_defs.assets or [])
        if self.enable_source_freshness_policies:
            updated_assets = self._apply_source_freshness_policies(updated_assets, manifest)

        if additional_assets or self.enable_source_freshness_policies:
            return dg.Definitions(
                assets=[*updated_assets, *additional_assets],
                resources=base_defs.resources,
                schedules=base_defs.schedules,
                sensors=base_defs.sensors,
            )

        return base_defs

    def _create_external_assets(
        self, manifest: Mapping[str, Any]
    ) -> list[dg.AssetSpec]:
        external_specs: list[dg.AssetSpec] = []

        for node_id, node_info in manifest.get("nodes", {}).items():
            package_name = node_info.get("package_name", "")

            if package_name not in self.external_packages:
                continue

            if node_info.get("resource_type") != "model":
                continue

            # Only expose public models (respecting dbt mesh access modifiers)
            if node_info.get("access", "protected") != "public":
                continue

            package_config = self.external_packages[package_name]
            key_prefix = package_config.get("key_prefix", [package_name])
            group_name = package_config.get("group_name", package_name)

            asset_key = dg.AssetKey([*key_prefix, node_info["name"]])

            external_specs.append(
                dg.AssetSpec(
                    key=asset_key,
                    group_name=group_name,
                    description=node_info.get(
                        "description", f"External model from {package_name}"
                    ),
                    metadata={
                        "dbt/package": package_name,
                        "dbt/original_file_path": node_info.get(
                            "original_file_path", ""
                        ),
                    },
                )
            )

        return external_specs

    def _create_exposure_assets(
        self, manifest: Mapping[str, Any]
    ) -> list[dg.AssetSpec]:
        """Create non-materializable assets for dbt exposures.

        Exposures represent downstream consumers (dashboards, ML models, APIs).
        They appear as leaf nodes in the asset graph with deps pointing to the
        dbt models they consume.
        """
        exposure_specs: list[dg.AssetSpec] = []

        for exposure_id, exposure_info in manifest.get("exposures", {}).items():
            name = exposure_info.get("name", "")
            exposure_type = exposure_info.get("type", "dashboard")

            # Build deps from the exposure's depends_on
            deps: list[dg.AssetDep] = []
            for dep_id in exposure_info.get("depends_on", {}).get("nodes", []):
                node = manifest.get("nodes", {}).get(dep_id)
                if node:
                    asset_key = self.translator.get_asset_key(node)
                    deps.append(dg.AssetDep(asset=asset_key))

            if not deps:
                continue

            asset_key = dg.AssetKey(["exposure", name])

            metadata: dict[str, Any] = {
                "dbt/exposure_type": exposure_type,
                "dbt/owner": exposure_info.get("owner", {}).get("name", ""),
            }
            url = exposure_info.get("url")
            if url:
                metadata["url"] = dg.MetadataValue.url(url)

            exposure_specs.append(
                dg.AssetSpec(
                    key=asset_key,
                    deps=deps,
                    group_name=self.exposure_group,
                    description=exposure_info.get(
                        "description", f"dbt {exposure_type}: {name}"
                    ),
                    metadata=metadata,
                    kinds={exposure_type},
                )
            )

        return exposure_specs

    def _apply_source_freshness_policies(
        self,
        assets: list[Any],
        manifest: Mapping[str, Any],
    ) -> list[Any]:
        """Apply FreshnessPolicy to source assets based on dbt freshness config.

        Reads warn_after/error_after from dbt source definitions and converts
        them to Dagster FreshnessPolicy(maximum_lag_minutes=...). Dagster
        passively evaluates these — no dbt execution needed.
        """
        # Build lookup: source asset key → freshness minutes
        freshness_by_source: dict[str, int] = {}
        for source_id, source_info in manifest.get("sources", {}).items():
            freshness = source_info.get("freshness", {})
            if not freshness:
                continue

            # Use warn_after as the policy (more conservative than error_after)
            warn_after = freshness.get("warn_after")
            error_after = freshness.get("error_after")

            threshold = warn_after or error_after
            if not threshold:
                continue

            count = threshold.get("count", 0)
            period = threshold.get("period", "hour")
            if period == "minute":
                minutes = count
            elif period == "hour":
                minutes = count * 60
            elif period == "day":
                minutes = count * 60 * 24
            else:
                continue

            asset_key = self.translator.get_asset_key(source_info)
            freshness_by_source[str(asset_key)] = minutes

        if not freshness_by_source:
            return assets

        # Apply policies to matching assets
        updated: list[Any] = []
        for asset in assets:
            if isinstance(asset, dg.AssetSpec) and str(asset.key) in freshness_by_source:
                minutes = freshness_by_source[str(asset.key)]
                asset = asset.replace_attributes(
                    freshness_policy=dg.FreshnessPolicy(maximum_lag_minutes=minutes),
                )
            updated.append(asset)

        return updated

    # Group resolution and partition definitions are handled by _MeshTranslator
