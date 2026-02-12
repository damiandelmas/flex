"""
Processor chain — declarative, composable pipelines.

Enables reorderable, testable,
error-tolerant processing stages.

Usage:
    chain = Chain([
        ParseProcessor(),
        FilterProcessor(),
        RankProcessor() if config.rank else None,  # conditional
    ])

    result = chain.execute(Context(query=q, results=[]))

Processors implement the Processor protocol:
    def process(self, ctx: T) -> T: ...

Chain filters None processors (conditional stages) and catches
errors per-processor (logs in ctx.metadata, continues pipeline).
"""

from typing import Protocol, TypeVar, List, Dict, Any, runtime_checkable
from dataclasses import dataclass, field


T = TypeVar('T')


@runtime_checkable
class Processor(Protocol):
    """Single pipeline stage. Receives context, returns updated context."""

    def process(self, ctx: Any) -> Any:
        ...


@dataclass
class PipelineContext:
    """Base context passed through processor chain.

    Subclass for domain-specific contexts (IndexContext, QueryContext).
    """

    results: List[Dict[str, Any]] = field(default_factory=list)
    """Accumulated results."""

    metadata: Dict[str, Any] = field(default_factory=dict)
    """Pipeline metadata (timing, errors, stage info)."""


class Chain:
    """Sequential processor chain executor.

    - Filters None processors (conditional stages)
    - Catches errors per-processor (logs, continues)
    - Returns final context after all stages
    """

    def __init__(self, processors: list):
        self.processors = [p for p in processors if p is not None]

    def execute(self, ctx):
        """Execute all processors in sequence."""
        for processor in self.processors:
            try:
                ctx = processor.process(ctx)
            except Exception as e:
                errors = ctx.metadata.setdefault('errors', [])
                errors.append({
                    'processor': processor.__class__.__name__,
                    'error': str(e),
                })
        return ctx

    def __repr__(self):
        names = [p.__class__.__name__ for p in self.processors]
        return f"Chain({' -> '.join(names)})"
