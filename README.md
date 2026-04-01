# ANVA Data Engineer SQL Case — Team Data

## Overview

This repository contains the solution for the ANVA Data Engineer SQL assignment (Team Data). The assignment involves designing and implementing a complete ETL pipeline in SQL, based on a JSON data source, within a layered data warehouse architecture.

The solution is written in **standard MySQL 8.0** — no graphical ETL tools, no platform-specific syntax beyond MySQL.

---

## Assignment Summary

The case covers three questions:

| Question | Description |
|---|---|
| Q1 | Build a complete ETL pipeline for the `PERSON` entity from a JSON source |
| Q2 | Design and implement the `ADDRESS` entity with full referential integrity |
| Q3 | Design a flexible data model for "free fields" that differ per customer |

---

## Architecture

All three questions follow a four-layer data warehouse architecture:

```
JSON Source
    │
    ▼
STAGING      ← raw landing zone, JSON stored as-is
    │
    ▼
DWH / ODS    ← validated, typed, deduplicated data
    │
    ▼
HIST         ← immutable history, every version preserved
    │
    ▼
DATAMART     ← business-ready views for analysts and BI tools
```