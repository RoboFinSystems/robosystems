"""Knowledge artifact builders for graph-based confidence refinement.

Generates precomputed Parquet artifacts from the full DuckDB staging database:
  - element_knowledge.parquet: Graph-structural signals per element qname
  - structure_profiles.parquet: Element frequency distributions per canonical_type
  - structure_consensus.parquet: Cross-filing majority-vote for identical structures
  - disclosure_profiles.parquet: Element frequency distributions per disclosure type
  - disclosure_consensus.parquet: Cross-filing majority-vote for disclosure types

These artifacts are lazy-loaded by the SemanticEnricher during per-filing
processing to refine confidence scores using graph-structural signals.
"""

from __future__ import annotations

import gc
import logging
import os
from collections import Counter
from pathlib import Path

import networkit as nk
import pyarrow as pa
import pyarrow.parquet as pq

from robosystems.adapters.sec.knowledge.classifiers import StatementClassifier
from robosystems.adapters.sec.knowledge.extractors import ArcExtractor
from robosystems.adapters.sec.knowledge.graphs import build_element_graph_from_arrow

logger = logging.getLogger(__name__)


def _log_memory(label: str) -> None:
  """Log current and peak process RSS memory usage.

  On Linux (ECS/Fargate), reads /proc/self/status for current VmRSS and peak VmHWM.
  Falls back to resource.getrusage for peak RSS on macOS.
  """
  try:
    proc_status = Path("/proc/self/status")
    if proc_status.exists():
      # Linux: read current and peak RSS from /proc
      status = proc_status.read_text()
      vm_rss = vm_hwm = None
      for line in status.splitlines():
        if line.startswith("VmRSS:"):
          vm_rss = int(line.split()[1]) / (1024 * 1024)  # kB -> GB
        elif line.startswith("VmHWM:"):
          vm_hwm = int(line.split()[1]) / (1024 * 1024)  # kB -> GB
      if vm_rss is not None and vm_hwm is not None:
        logger.info(f"[memory] {label}: {vm_rss:.2f} GB current, {vm_hwm:.2f} GB peak")
      elif vm_rss is not None:
        logger.info(f"[memory] {label}: {vm_rss:.2f} GB current")
      return

    # macOS fallback: only peak RSS available
    import resource

    rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if os.uname().sysname == "Darwin":
      rss_gb = rss_bytes / (1024**3)
    else:
      rss_gb = rss_bytes / (1024**2)
    logger.info(f"[memory] {label}: {rss_gb:.2f} GB peak RSS")
  except Exception:
    logger.debug("Memory logging failed", exc_info=True)


class ElementKnowledgeBuilder:
  """Generates the element knowledge artifact from a DuckDB staging database.

  Uses SQL-side deduplication to keep Python memory under ~2.2 GB
  regardless of corpus size.

  Output schema:
    qname (STRING), primary_statement (STRING), bfs_depth (INT32),
    pagerank (FLOAT), core_number (INT32), neighborhood_agreement (FLOAT),
    filing_count (INT32), disclosure_type (STRING)
  """

  def __init__(self, memory_limit: str = "8GB") -> None:
    self._memory_limit = memory_limit

  def build(self, db_path: str | Path) -> Path:
    """Build the element knowledge artifact and write to ARTIFACT_PATH.

    Memory-conscious: frees intermediate results aggressively between steps.
    DuckDB uses spill-to-disk with a low memory limit; Python objects are
    deleted and gc.collect()'d as soon as they're no longer needed.

    Args:
        db_path: Path to the DuckDB staging database.

    Returns:
        Path to the written element_knowledge.parquet file.
    """
    from robosystems.config.storage.shared import get_artifact_path

    _log_memory("start")
    extractor = ArcExtractor(db_path, memory_limit=self._memory_limit)

    # --- Phase 1: Extract from DuckDB (connections open/close per call) ---

    logger.info("Extracting graph via Arrow zero-copy path")
    nodes, edges_arrow = extractor.extract_graph_arrow()
    logger.info(f"Extracted {len(nodes)} nodes, {edges_arrow.num_rows} edges (Arrow)")
    _log_memory("after extract_graph_arrow")

    logger.info("Extracting element filing counts")
    filing_counts = extractor.extract_element_filing_counts()
    logger.info(f"Extracted filing counts for {len(filing_counts)} elements")
    _log_memory("after extract_element_filing_counts")

    logger.info("Extracting disclosure classifications")
    disclosure_types = extractor.extract_element_disclosure_types()
    disclosure_roots = extractor.extract_disclosure_root_elements()
    logger.info(
      f"Extracted disclosure types for {len(disclosure_types)} elements, "
      f"{len(disclosure_roots)} disclosure roots"
    )
    _log_memory("after disclosure extractions")

    # Done with DuckDB — extractor holds no state
    del extractor
    gc.collect()
    _log_memory("after extractor cleanup")

    # --- Phase 2: Build graph (frees Arrow arrays immediately after) ---

    logger.info("Building element graph (Arrow → CSR)")
    element_graph = build_element_graph_from_arrow(nodes, edges_arrow)
    logger.info(
      f"Graph: {element_graph.num_nodes} nodes, {element_graph.num_edges} edges"
    )

    # Arrow arrays are copied into the CSR — free them
    del nodes, edges_arrow
    gc.collect()
    _log_memory("after graph build + arrow cleanup")

    # --- Phase 3: Structural analysis (free each result before next) ---

    logger.info("Running PageRank")
    pagerank_scores = self._run_pagerank(element_graph)
    _log_memory("after PageRank")

    logger.info("Running core decomposition")
    core_numbers = self._run_core_decomposition(element_graph)
    _log_memory("after core decomposition")

    logger.info("Running BFS classification")
    classifications = StatementClassifier().classify(
      element_graph, disclosure_roots=disclosure_roots or None
    )
    del disclosure_roots
    gc.collect()
    _log_memory("after BFS classification")

    logger.info("Computing neighborhood agreement")
    agreement_scores = self._compute_neighborhood_agreement(
      element_graph, classifications
    )
    _log_memory("after neighborhood agreement")

    # --- Phase 4: Build output rows (then free graph + analysis dicts) ---

    logger.info(
      f"Building output rows: {len(element_graph.elements)} elements, "
      f"{classifications.total_classified} classified, "
      f"{classifications.total_unclassified} unclassified"
    )
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

      min_depth = classifications.get_min_depth(qname)
      bfs_depths.append(min_depth)

      idx = element_graph.get_idx(qname)
      pageranks.append(pagerank_scores.get(idx, 0.0))
      cores.append(core_numbers.get(idx, 0))
      agreements.append(agreement_scores.get(qname, 0.0))
      f_counts.append(filing_counts.get(qname, 0))
      d_types.append(disclosure_types.get(qname))

    # Free all analysis objects — only column lists needed for Parquet write
    del element_graph, pagerank_scores, core_numbers, classifications
    del agreement_scores, filing_counts, disclosure_types
    gc.collect()
    _log_memory("after building output rows + cleanup")

    # --- Phase 5: Write Parquet ---
    logger.info(f"Writing parquet with {len(qnames)} rows")

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

    result = {i: s / max_score for i, s in enumerate(scores)}
    del pr, scores
    return result

  def _run_core_decomposition(self, element_graph) -> dict[int, int]:
    """Run k-core decomposition on the element graph."""
    graph = element_graph.graph
    if graph.numberOfNodes() == 0:
      return {}

    undirected = nk.graphtools.toUndirected(graph)
    core = nk.centrality.CoreDecomposition(undirected)
    core.run()
    scores = {i: int(s) for i, s in enumerate(core.scores())}

    # Free the undirected copy immediately (can be as large as the original graph)
    del undirected, core
    gc.collect()

    return scores

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

  def __init__(self, memory_limit: str = "8GB") -> None:
    self._memory_limit = memory_limit

  def build(self, db_path: str | Path) -> tuple[Path, Path]:
    """Build both structure knowledge artifacts.

    Args:
        db_path: Path to the DuckDB staging database.

    Returns:
        Tuple of (profiles_path, consensus_path).
    """
    _log_memory("structure builder start")
    extractor = ArcExtractor(db_path, memory_limit=self._memory_limit)

    logger.info("Extracting structure compositions")
    compositions = extractor.extract_structure_compositions()
    logger.info(f"Extracted {len(compositions)} structure compositions")
    _log_memory("after structure extraction")

    profiles_path = self._compute_profiles(compositions)
    consensus_path = self._compute_consensus(compositions)
    _log_memory("after structure profiles + consensus")

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


class DisclosureProfileBuilder:
  """Generates disclosure classification artifacts from a DuckDB staging database.

  Uses structures where DISCLOSURE_CONCEPT_MAP matched (labeled training data)
  to build element composition profiles per disclosure type. Element importance
  is weighted by PageRank from the element_knowledge artifact, making
  high-centrality elements more discriminative.

  Produces two artifacts:
  1. disclosure_profiles.parquet — element frequency distributions per disclosure type,
     weighted by element_knowledge PageRank scores
  2. disclosure_consensus.parquet — cross-filing majority-vote using disclosure type
  """

  def __init__(self, memory_limit: str = "8GB") -> None:
    self._memory_limit = memory_limit

  def build(self, db_path: str | Path) -> tuple[Path, Path]:
    """Build both disclosure knowledge artifacts.

    Args:
        db_path: Path to the DuckDB staging database.

    Returns:
        Tuple of (profiles_path, consensus_path).
    """
    _log_memory("disclosure builder start")
    extractor = ArcExtractor(db_path, memory_limit=self._memory_limit)

    logger.info("Extracting disclosure compositions")
    compositions = extractor.extract_disclosure_compositions()
    logger.info(f"Extracted {len(compositions)} disclosure compositions")
    _log_memory("after disclosure extraction")

    pagerank_scores = self._load_pagerank_scores()
    if pagerank_scores:
      logger.info(f"Loaded PageRank scores for {len(pagerank_scores)} elements")
    else:
      logger.info("PageRank scores unavailable, using frequency-only weighting")

    profiles_path = self._compute_profiles(compositions, pagerank_scores)
    consensus_path = self._compute_consensus(compositions)
    _log_memory("after disclosure profiles + consensus")

    return profiles_path, consensus_path

  def _load_pagerank_scores(self) -> dict[str, float]:
    """Load PageRank scores from element_knowledge artifact.

    Reads directly via file I/O to avoid circular imports with enrichment.py.

    Returns:
        Dict mapping qname to normalized PageRank score.
        Empty dict if artifact not available.
    """
    from robosystems.config.storage.shared import get_artifact_path

    path = Path(get_artifact_path("element_knowledge"))
    if not path.exists():
      return {}

    try:
      table = pq.read_table(path, columns=["qname", "pagerank"])
      cols = table.to_pydict()
      return {
        cols["qname"][i]: cols["pagerank"][i] or 0.0 for i in range(len(cols["qname"]))
      }
    except Exception as e:
      logger.debug(f"Failed to load PageRank scores: {e}")
      return {}

  def _compute_profiles(
    self,
    compositions: list[tuple[str, str, str, list[str]]],
    pagerank_scores: dict[str, float],
  ) -> Path:
    """Compute element frequency distributions per disclosure type.

    For each disclosure type, compute how often each element qname appears
    across all structures of that type. Weighted scores incorporate
    PageRank centrality to make important elements more discriminative.
    """
    from robosystems.config.storage.shared import get_artifact_path

    # Group structures by disclosure_type
    type_structures: dict[str, list[list[str]]] = {}
    for _sid, disclosure_type, _def_hash, element_qnames in compositions:
      type_structures.setdefault(disclosure_type, []).append(element_qnames)

    # Compute frequency and weighted score per element per type
    disclosure_types = []
    qnames = []
    frequencies = []
    weighted_scores = []
    structure_counts = []

    for dt, structure_lists in type_structures.items():
      total = len(structure_lists)
      if total == 0:
        continue

      element_counts: Counter[str] = Counter()
      for elements in structure_lists:
        for qname in set(elements):
          element_counts[qname] += 1

      for qname, count in element_counts.items():
        freq = count / total
        pr = pagerank_scores.get(qname, 0.0)
        disclosure_types.append(dt)
        qnames.append(qname)
        frequencies.append(freq)
        weighted_scores.append(freq * (1.0 + pr))
        structure_counts.append(total)

    table = pa.table(
      {
        "disclosure_type": pa.array(disclosure_types, type=pa.string()),
        "qname": pa.array(qnames, type=pa.string()),
        "frequency": pa.array(frequencies, type=pa.float64()),
        "weighted_score": pa.array(weighted_scores, type=pa.float64()),
        "structure_count": pa.array(structure_counts, type=pa.int32()),
      }
    )

    output_path = Path(get_artifact_path("disclosure_profiles"))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
      pq.write_table(table, f, compression="snappy")

    logger.info(
      f"Disclosure profiles: {len(disclosure_types)} rows, "
      f"{len(type_structures)} types -> {output_path}"
    )
    return output_path

  def _compute_consensus(
    self,
    compositions: list[tuple[str, str, str, list[str]]],
  ) -> Path:
    """Compute cross-filing majority-vote for identical structure definitions.

    Groups structures by definition_hash and finds the majority-vote
    disclosure_type for each group.
    """
    from robosystems.config.storage.shared import get_artifact_path

    hash_votes: dict[str, list[str]] = {}
    for _sid, disclosure_type, def_hash, _elements in compositions:
      hash_votes.setdefault(def_hash, []).append(disclosure_type)

    definition_hashes = []
    consensus_types = []
    consensus_ratios = []
    filing_counts = []

    for def_hash, votes in hash_votes.items():
      if not votes:
        continue

      total = len(votes)
      vote_counts = Counter(votes)
      winner, winner_count = vote_counts.most_common(1)[0]

      definition_hashes.append(def_hash)
      consensus_types.append(winner)
      consensus_ratios.append(winner_count / total)
      filing_counts.append(total)

    table = pa.table(
      {
        "definition_hash": pa.array(definition_hashes, type=pa.string()),
        "disclosure_type": pa.array(consensus_types, type=pa.string()),
        "consensus_ratio": pa.array(consensus_ratios, type=pa.float64()),
        "filing_count": pa.array(filing_counts, type=pa.int32()),
      }
    )

    output_path = Path(get_artifact_path("disclosure_consensus"))
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "wb") as f:
      pq.write_table(table, f, compression="snappy")

    logger.info(
      f"Disclosure consensus: {len(definition_hashes)} entries -> {output_path}"
    )
    return output_path
