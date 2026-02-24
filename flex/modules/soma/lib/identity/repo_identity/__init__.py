"""Repo Identity - Git repository tracker with stable identity by root_commit."""

from .identity import RepoIdentity, Repo

# Backward compat alias
GitRegistry = RepoIdentity

__all__ = ["RepoIdentity", "Repo", "GitRegistry"]
