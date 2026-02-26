# TASK

Spawn the `flx-trace` subagent to investigate the following query against the user's knowledge cells:

$ARGUMENTS

The agent will orient, explore, and return a synthesis. Compile an appropriate response to the USER.

## Depth

**VERY IMPORTANT:** THIS SIGNALS THE DEPTH OF RESEARCH FOR THE USERS.

FOR QUICK: JUST SURFACE A QUICK ANSWER. USE ONE TO THREE QUERIES.

FOR EXHAUSTIVE SEARCH: DO EXTENSIVE AND EXPLORATIVE RESEARCH USING FLEX. RETURN A COMPLETE REPORT.

Count the o's after the g in the go signal:

```
go         1o    Quick      @orient + 1-2 queries. One finding. Done.
goo        2o    Moderate   @orient + 3-5 queries. Escalate specificity. Structured summary.
gooo       3o    Deep       @orient + 5-10 queries. Cross-cell if useful. Full narrative.
gooo[Nxo]  3+N   Exhaustive N scales breadth — lineage, hubs, communities, full report.
```