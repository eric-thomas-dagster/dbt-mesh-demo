"""Custom dbt component that handles dbt mesh cross-project references.

When a dbt project includes models from an external package (via dbt mesh),
this component creates external asset specs for those models so that Dagster
can track lineage across project boundaries without duplicating assets.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Optional

import dagster as dg
from dagster.components.resolved.model import Resolver
from dagster_dbt import DbtProject, DbtProjectComponent


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

    def _resolve_group(self, node_info: Mapping[str, Any]) -> str | None:
        """Resolve group name from overrides, falling back to dbt group."""
        if not self.group_overrides:
            return None

        # Check resource_type first (e.g. "seed", "model", "test")
        resource_type = node_info.get("resource_type", "")
        if resource_type in self.group_overrides:
            return self.group_overrides[resource_type]

        # Then check each fqn segment (e.g. ["silver_project", "staging", "stg_customers"])
        for segment in node_info.get("fqn", []):
            if segment in self.group_overrides:
                return self.group_overrides[segment]

        return None

    def get_asset_spec(
        self,
        manifest: Mapping[str, Any],
        unique_id: str,
        project: Optional[DbtProject],
    ) -> dg.AssetSpec:
        base_spec = super().get_asset_spec(manifest, unique_id, project)

        node_info = self.get_resource_props(manifest, unique_id)
        group_override = self._resolve_group(node_info)

        if group_override:
            return base_spec.replace_attributes(group_name=group_override)

        return base_spec
