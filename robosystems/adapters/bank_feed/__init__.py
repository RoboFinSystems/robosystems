"""What every bank feed shares, whatever the source.

A bank feed captures posted bank activity into the inbox against the
tenant's existing chart; it authors no GL rows. Each source keeps its own
client, category tables and transform.
"""
