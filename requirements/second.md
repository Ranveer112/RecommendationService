# Recommendation Service — MCP Server

## Overview

Expose the Recommendation Service as a **Model Context Protocol (MCP) server** so that AI assistants and LLM-based agents can discover and invoke its capabilities (catalog management, ratings, recommendations, metrics) as tool calls within a conversational context.

## Goals

- Wrap existing API functionality as MCP-compatible **tools**.
- Support tool discovery, parameter schemas, and structured responses per the MCP specification.
- Keep the implementation lightweight — reuse current service logic; add only the MCP transport layer.

## Open Questions

- Should all existing endpoints be exposed as tools, or a curated subset?
- Authentication and access-control model for MCP clients.
