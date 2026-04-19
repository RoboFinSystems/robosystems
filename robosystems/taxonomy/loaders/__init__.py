"""Loaders: parse taxonomy artifacts into Pydantic TaxonomyPackage."""

from __future__ import annotations

from robosystems.taxonomy.loaders.jsonld_loader import load_taxonomy_package

__all__ = ["load_taxonomy_package"]
