"""Knowledge artifact builders for graph-based confidence refinement.

Generates precomputed Parquet artifacts from the full DuckDB staging database:
  - element_knowledge.parquet: Graph-structural signals per element qname
  - structure_profiles.parquet: Element frequency distributions per canonical_type
  - structure_consensus.parquet: Cross-filing majority-vote for identical structures

These artifacts are lazy-loaded by the SemanticEnricher during per-filing
processing to refine confidence scores using graph-structural signals.
"""

from __future__ import annotations

import logging
from collections import Counter
from pathlib import Path

import networkit as nk
import pyarrow as pa
import pyarrow.parquet as pq

from robosystems.adapters.sec.knowledge.classifiers import StatementClassifier
from robosystems.adapters.sec.knowledge.extractors import ArcExtractor
from robosystems.adapters.sec.knowledge.graphs import build_element_graph_from_arrow

logger = logging.getLogger(__name__)


class ElementKnowledgeBuilder:
  """Generates the element knowledge artifact from a DuckDB staging database.

  Uses SQL-side deduplication to keep Python memory under ~2.2 GB
  regardless of corpus size.

  Output schema:
    qname (STRING), primary_statement (STRING), bfs_depth (INT32),
    pagerank (FLOAT), core_number (INT32), neighborhood_agreement (FLOAT),
    filing_count (INT32), disclosure_type (STRING)
  """

  def __init__(self, memory_limit: str = "10GB") -> None:
    self._memory_limit = memory_limit

  def build(self, db_path: str | Path) -> Path:
    """Build the element knowledge artifact and write to ARTIFACT_PATH.

    Args:
        db_path: Path to the DuckDB staging database.

    Returns:
        Path to the written element_knowledge.parquet file.
    """
    from robosystems.config.storage.shared import get_artifact_path

    extractor = ArcExtractor(db_path, memory_limit=self._memory_limit)

    # Extract graph as Arrow arrays (zero-copy DuckDB → Arrow → CSR)
    logger.info("Extracting graph via Arrow zero-copy path")
    nodes, edges_arrow = extractor.extract_graph_arrow()
    logger.info(f"Extracted {len(nodes)} nodes, {edges_arrow.num_rows} edges (Arrow)")

    logger.info("Extracting element filing counts")
    filing_counts = extractor.extract_element_filing_counts()
    logger.info(f"Extracted filing counts for {len(filing_counts)} elements")

    # Extract disclosure classifications
    logger.info("Extracting disclosure classifications")
    disclosure_types = extractor.extract_element_disclosure_types()
    disclosure_roots = extractor.extract_disclosure_root_elements()
    logger.info(
      f"Extracted disclosure types for {len(disclosure_types)} elements, "
      f"{len(disclosure_roots)} disclosure roots"
    )

    # Build graph from Arrow arrays via CSR
    logger.info("Building element graph (Arrow → CSR)")
    element_graph = build_element_graph_from_arrow(nodes, edges_arrow)
    logger.info(
      f"Graph: {element_graph.num_nodes} nodes, {element_graph.num_edges} edges"
    )

    # Structural analysis
    logger.info("Running PageRank")
    pagerank_scores = self._run_pagerank(element_graph)

    logger.info("Running core decomposition")
    core_numbers = self._run_core_decomposition(element_graph)

    logger.info("Running BFS classification")
    classifications = StatementClassifier().classify(
      element_graph, disclosure_roots=disclosure_roots or None
    )

    logger.info("Computing neighborhood agreement")
    agreement_scores = self._compute_neighborhood_agreement(
      element_graph, classifications
    )

    # Build rows
    qnames = []
    primary_statements = []
    bfs_depths = []
    pageranks = []
    cores = []
    agreements = []
    f_counts = []
    d_types = []

    for qname in element_graph.elements:
      qnames.append(qname)

      stmt = classifications.get_primary_statement(qname)
      primary_statements.append(stmt.value if stmt else None)

      entries = classifications.classifications.get(qname)
      if entries:
        bfs_depths.append(min(e.depth for e in entries))
      else:
        bfs_depths.append(None)

      idx = element_graph.get_idx(qname)
      pageranks.append(pagerank_scores.get(idx, 0.0))
      cores.append(core_numbers.get(idx, 0))
      agreements.append(agreement_scores.get(qname, 0.0))
      f_counts.append(filing_counts.get(qname, 0))
      d_types.append(disclosure_types.get(qname))

    # Write Parquet
    table = pa.table(
      {
        "qname": pa.array(qnames, type=pa.string()),
        "primary_statement": pa.array(primary_statements, type=pa.string()),
        "bfs_depth": pa.array(bfs_depths, type=pa.int32()),
        "pagerank": pa.array(pageranks, type=pa.float64()),
        "core_number": pa.array(cores, type=pa.int32()),
        "neighborhood_agreement": pa.array(agreements, type=pa.float64()),
        "filing_count": pa.array(f_counts, type=pa.int32()),
        "disclosure_type": pa.array(d_types, type=pa.string()),
      }
    )

    output_path = Path(get_artifact_path("element_knowledge"))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
      pq.write_table(table, f, compression="snappy")

    logger.info(f"Element knowledge artifact: {len(qnames)} elements -> {output_path}")
    return output_path

  def _run_pagerank(self, element_graph) -> dict[int, float]:
    """Run PageRank on the element graph."""
    graph = element_graph.graph
    if graph.numberOfNodes() == 0:
      return {}

    pr = nk.centrality.PageRank(graph, damp=0.85, tol=1e-6)
    pr.run()
    scores = pr.scores()

    # Normalize to 0-1
    max_score = max(scores) if scores else 1.0
    if max_score == 0:
      max_score = 1.0

    return {i: s / max_score for i, s in enumerate(scores)}

  def _run_core_decomposition(self, element_graph) -> dict[int, int]:
    """Run k-core decomposition on the element graph."""
    graph = element_graph.graph
    if graph.numberOfNodes() == 0:
      return {}

    undirected = nk.graphtools.toUndirected(graph)
    core = nk.centrality.CoreDecomposition(undirected)
    core.run()

    return {i: int(s) for i, s in enumerate(core.scores())}

  def _compute_neighborhood_agreement(
    self, element_graph, classifications
  ) -> dict[str, float]:
    """Compute fraction of neighbors sharing the same primary statement."""
    graph = element_graph.graph
    result = {}

    for qname in element_graph.elements:
      idx = element_graph.get_idx(qname)
      if idx is None:
        continue

      my_stmt = classifications.get_primary_statement(qname)
      if my_stmt is None:
        result[qname] = 0.0
        continue

      neighbors = list(graph.iterNeighbors(idx))
      if not neighbors:
        result[qname] = 0.0
        continue

      agree = 0
      for n_idx in neighbors:
        n_qname = element_graph.get_qname(n_idx)
        n_stmt = classifications.get_primary_statement(n_qname)
        if n_stmt == my_stmt:
          agree += 1

      result[qname] = agree / len(neighbors)

    return result


class StructureKnowledgeBuilder:
  """Generates structure classification artifacts from a DuckDB staging database.

  Produces two artifacts:
  1. structure_profiles.parquet — element frequency distributions per canonical_type
  2. structure_consensus.parquet — cross-filing majority-vote for identical structures
  """

  def __init__(self, memory_limit: str = "10GB") -> None:
    self._memory_limit = memory_limit

  def build(self, db_path: str | Path) -> tuple[Path, Path]:
    """Build both structure knowledge artifacts.

    Args:
        db_path: Path to the DuckDB staging database.

    Returns:
        Tuple of (profiles_path, consensus_path).
    """
    extractor = ArcExtractor(db_path, memory_limit=self._memory_limit)

    logger.info("Extracting structure compositions")
    compositions = extractor.extract_structure_compositions()
    logger.info(f"Extracted {len(compositions)} structure compositions")

    profiles_path = self._compute_profiles(compositions)
    consensus_path = self._compute_consensus(compositions)

    return profiles_path, consensus_path

  def _compute_profiles(
    self,
    compositions: list[tuple[str, str | None, str, list[str]]],
  ) -> Path:
    """Compute element frequency distributions per canonical_type.

    For each canonical_type, compute how often each element qname appears
    across all structures of that type.
    """
    from robosystems.config.storage.shared import get_artifact_path

    # Group structures by canonical_type
    type_structures: dict[str, list[list[str]]] = {}
    for _sid, canonical_type, _def_hash, element_qnames in compositions:
      if canonical_type is None:
        continue
      type_structures.setdefault(canonical_type, []).append(element_qnames)

    # Compute frequency of each element per type
    canonical_types = []
    qnames = []
    frequencies = []
    structure_counts = []

    for ct, structure_lists in type_structures.items():
      total = len(structure_lists)
      if total == 0:
        continue

      # Count how many structures contain each element
      element_counts: Counter[str] = Counter()
      for elements in structure_lists:
        for qname in set(elements):  # set() to count presence, not multiplicity
          element_counts[qname] += 1

      for qname, count in element_counts.items():
        canonical_types.append(ct)
        qnames.append(qname)
        frequencies.append(count / total)
        structure_counts.append(total)

    table = pa.table(
      {
        "canonical_type": pa.array(canonical_types, type=pa.string()),
        "qname": pa.array(qnames, type=pa.string()),
        "frequency": pa.array(frequencies, type=pa.float64()),
        "structure_count": pa.array(structure_counts, type=pa.int32()),
      }
    )

    output_path = Path(get_artifact_path("structure_profiles"))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
      pq.write_table(table, f, compression="snappy")

    logger.info(
      f"Structure profiles: {len(canonical_types)} rows, "
      f"{len(type_structures)} types -> {output_path}"
    )
    return output_path

  def _compute_consensus(
    self,
    compositions: list[tuple[str, str | None, str, list[str]]],
  ) -> Path:
    """Compute cross-filing majority-vote for identical structure definitions.

    Groups structures by definition_hash and finds the majority-vote
    canonical_type for each group.
    """
    from robosystems.config.storage.shared import get_artifact_path

    # Group by definition_hash
    hash_votes: dict[str, list[str | None]] = {}
    for _sid, canonical_type, def_hash, _elements in compositions:
      hash_votes.setdefault(def_hash, []).append(canonical_type)

    definition_hashes = []
    consensus_types = []
    consensus_ratios = []
    filing_counts = []

    for def_hash, votes in hash_votes.items():
      non_null = [v for v in votes if v is not None]
      if not non_null:
        continue

      total = len(non_null)
      vote_counts = Counter(non_null)
      winner, winner_count = vote_counts.most_common(1)[0]

      definition_hashes.append(def_hash)
      consensus_types.append(winner)
      consensus_ratios.append(winner_count / total)
      filing_counts.append(len(votes))

    table = pa.table(
      {
        "definition_hash": pa.array(definition_hashes, type=pa.string()),
        "canonical_type": pa.array(consensus_types, type=pa.string()),
        "consensus_ratio": pa.array(consensus_ratios, type=pa.float64()),
        "filing_count": pa.array(filing_counts, type=pa.int32()),
      }
    )

    output_path = Path(get_artifact_path("structure_consensus"))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
      pq.write_table(table, f, compression="snappy")

    logger.info(
      f"Structure consensus: {len(definition_hashes)} entries -> {output_path}"
    )
    return output_path
