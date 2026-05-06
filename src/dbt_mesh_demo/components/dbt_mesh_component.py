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

        if not self.external_packages:
            return base_defs

        # Get the resolved project and its manifest
        project = self._project_manager.get_project(state_path)
        manifest_path = Path(project.manifest_path)

        if not manifest_path.exists():
            return base_defs

        manifest = json.loads(manifest_path.read_text())
        external_assets = self._create_external_assets(manifest)

        if external_assets:
            return dg.Definitions(
                assets=[*base_defs.assets, *external_assets],
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

    # Group resolution and partition definitions are handled by _MeshTranslator
