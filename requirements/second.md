# Recommendation Service — Phase 2 Requirements

---

## User Story 1: MCP Server Integration

**As an** AI assistant or LLM-based agent,
**I want** to discover and invoke the Recommendation Service's capabilities (catalog management, ratings, recommendations, metrics) as MCP tool calls,
**So that** I can seamlessly integrate product recommendations into conversational workflows.

### Acceptance Criteria

- Existing API functionality is wrapped as MCP-compatible **tools**.
- Tool discovery, parameter schemas, and structured responses conform to the MCP specification.
- Implementation is lightweight — reuses current service logic and adds only the MCP transport layer.

### Open Questions

- Should all existing endpoints be exposed as tools, or a curated subset?
- Authentication and access-control model for MCP clients.

---

## User Story 2: Recency-Weighted Predictions

**As an** API consumer,
**I want** user predictions that align heavier with their current taste than their past taste,
**So that** recommendations stay relevant as user preferences evolve over time.

### Acceptance Criteria

- The prediction algorithm applies a recency bias, giving more weight to recent user interactions.
- Older ratings or interactions decay in influence relative to newer ones.
- The weighting strategy is configurable (e.g., decay factor or time window).

### Open Questions

- What decay model should be used (exponential, linear, sliding window)?
- Should the recency bias be tunable per-user or a global setting?
