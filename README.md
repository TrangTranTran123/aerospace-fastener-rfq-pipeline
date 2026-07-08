# Aerospace Fastener RFQ-to-Fulfillment Pipeline (Apache Airflow)

A 3-DAG orchestration pipeline simulating an end-to-end quote-to-cash workflow for an AS9120/ASA-100-compliant aerospace parts distributor — from RFQ intake through certificate traceability matching to order fulfillment.

## Overview

Aerospace parts distribution has a compliance requirement that most e-commerce pipelines don't: every part shipped must be traceable to the **exact lot/serial number and its specific Certificate of Conformance** — not just any certificate for that part number. This project designs an Airflow pipeline architecture around that constraint, with a human quality sign-off gate built in rather than assumed away.

## Architecture

Airflow sits as the central orchestrator (hub-and-spoke), calling out independently to an ERP system (ERPNext — source of truth for inventory/lot/certificate data), Postgres (DAG/task state), S3 (certificate PDF storage), and an email service — rather than treating any single system as a one-time sequential handoff.

## The 3 DAGs

**1. `dag1_rfq_intake`**
Parses an incoming RFQ (Request for Quote), matches requested parts against ERPNext stock, and — on a successful match — triggers DAG 2 via `TriggerDagRunOperator`.

**2. `dag2_quote_cert_match`**
Matches the specific lot/serial number of available stock to its Certificate of Conformance (not just part-level matching). Sets a `quality_signoff_status` Airflow Variable to `pending`, creating a **deliberate human approval gate** before fulfillment can proceed — reflecting the reality that automated systems in regulated industries shouldn't fully bypass human quality sign-off.

**3. `dag3_order_fulfillment`**
Uses a `PythonSensor` to poll the `quality_signoff_status` Variable until a human approves it, then decrements stock, generates an invoice, and notifies the customer.

## Why the Human Approval Gate Matters

This is the key design decision in the project: rather than building a fully automated pipeline, DAG 2 deliberately pauses and waits for human quality sign-off before DAG 3 can fulfill the order. This reflects a real constraint in AS9120-regulated environments — automation should support compliance and audit trails, not remove human accountability from safety-critical decisions.

## Key Concepts Demonstrated
- Multi-DAG orchestration with `TriggerDagRunOperator` (cross-DAG dependencies)
- Human-in-the-loop workflow control via Airflow Variables + `PythonSensor` polling
- Hub-and-spoke integration pattern (Airflow orchestrating ERP, database, object storage, and notification systems independently, rather than one linear pipeline)
- Designing for regulatory traceability (lot/serial-level certificate matching, not part-level)

## Tech Stack
`Apache Airflow` · `Docker` · `Postgres` (DAG/task metadata) · ERPNext (simulated as source of truth) · `AWS S3` (certificate storage, conceptual)

## Files
- [`dag1_rfq_intake.py`](./dag1_rfq_intake.py)
- [`dag2_quote_cert_match.py`](./dag2_quote_cert_match.py)
- [`dag3_order_fulfillment.py`](./dag3_order_fulfillment.py)

> Note: This pipeline was built as a self-directed learning project to understand Airflow orchestration patterns applicable to aerospace parts distribution compliance workflows (AS9120/ASA-100), simulating realistic RFQ-to-fulfillment logic rather than connecting to a live ERP system.
