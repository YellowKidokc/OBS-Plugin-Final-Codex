# Obsidian Semantic AI Plugin (Master Specification)

## Overview
**Working name:** `obsidian-semantic-ai` (based on [`obsidian-note-definitions`](https://github.com/dominiclet/obsidian-note-definitions))

This document is the definitive developer brief for an Obsidian plugin that brings academic-grade semantic structure, AI-driven classification, and visual knowledge mapping into Markdown notes. It consolidates inline tagging, native or API-powered classification, customizable prompts, batch workflows, and Mermaid-based relationship graphs, while laying groundwork for Python/Postgres synchronization.

## Core Objectives
- Inline tagging with hidden structured metadata and UUIDs.
- Native AI engine to classify notes by epistemic structures.
- Editable prompt templates per tag type and custom classifiers.
- Batch classification with token cost estimation.
- Auto-generated Mermaid visualizations of semantic relationships.
- Multi-layer tagging (note → paragraph → sentence → term) with parent UUIDs.
- Right-click tooling for note and folder operations.
- Phase-2 readiness for Python listener + Postgres sync.

## Tagging & Hidden Structure
- AI-generated classifications append to notes as hard-coded blocks:
  ```
  %%tag::TYPE::UUID::"Label"::parent_UUID%%
  ```
- Supported tag types include: Axiom, Claim, Evidence Bundle, Scientific Process, Relationship, Internal Link, External Link, Proper Name, Forward Link, Word Ontology, Sentence, Paragraph.
- `parent_UUID` supports nesting (word → sentence → paragraph → note).
- Users can toggle visibility via command or right-click: **“Show All Hidden Tags.”** Tags remain in the file even when hidden.

## AI Classification (Native)
- Runs locally or via user-provided API key.
- Triggered from right-click or command palette: **“Run AI Classifier.”**
- Parses AI output into tag blocks and writes them to the note.

## Prompt Editing
- Settings include tabs for each tag type (Axioms, Claims, Evidence, etc.).
- Each tab provides:
  - Editable prompt template with defaults.
  - “Reset to default” control.
- Classification uses the active prompt for the selected type.

## Custom Classifiers
- **Custom** settings tab lets users define a keyword (e.g., “mouse”) and a prompt for handling it.
- When triggered, auto-creates tag blocks according to the custom logic.

## Batch Processing
- Right-click a folder or run command: **“Batch Classify Folder.”**
- Plugin counts notes, estimates token cost (for API use), and asks for confirmation.
- Real-time results panel shows progress, e.g.:
  ```
  📄 Processing: file.md
  ✅ Tagged: 3 Axioms, 1 Claim, 2 Evidence
  ```

## Mermaid Semantic Graphs
- After classification, auto-generate Mermaid diagrams reflecting tag relationships, for example:
  ```mermaid
  graph TD
    ax1["Axiom: Light is constant"]
    cl1["Claim: Time dilates"]
    ev1["Evidence: Hafele-Keating"]
    ax1 --> cl1 --> ev1
  ```
- Diagrams can be appended to the note or displayed in a right-hand panel.
- Command available: **“Regenerate Semantic Graph.”**

## UI/UX Layout
- **Settings tabs:** Prompt Editor (by type), Custom Classifiers, Tag Settings, Graph Settings.
- **Right-hand panel:** renders Mermaid diagrams, shows semantic layer summaries, and lets users explore relationships/UUID hierarchies.

## Right-Click Tools
- On notes: **Run AI Classifier**, **Show Hidden Tags**, **Open Semantic Map**, **Classify as: [Evidence / Axiom / Claim / ...]**
- On folders: **Batch Classify This Folder**

## Backend Sync Readiness (Phase 2)
- All tags include UUIDs to serve as global identifiers.
- Plugin must accept updates from a local Python listener and push changes to Postgres while maintaining tag consistency.

## Suggested File Structure
```
src/
  ai/
    classifier.ts
    prompt-manager.ts
  tagging/
    tag-writer.ts
    uuid-generator.ts
  ui/
    mermaid-view.ts
    result-panel.ts
    prompt-tabs.ts
  main.ts
  settings.ts
```

## Default Prompt Samples
- **Axiom:** Identify core foundational truths in this document. These are axioms — statements that do not rely on prior proof and support other claims.
- **Claim:** Identify any claims made by the author. A claim asserts a position that can be supported or refuted.
- **Evidence:** Identify evidence used to support claims or axioms. This may be empirical data, quotes, or logical arguments.
- **Relationship:** Identify explicit or implicit relationships between concepts, entities, or events in the text.
- **Word Ontology:** Identify specialized terms and link them to their definitions, origins, or ontological categories.

## Commands & Visibility
- **Run AI Classifier** (note context) — processes current note with active prompts.
- **Batch Classify Folder** — processes all notes with progress + cost estimate.
- **Show All Hidden Tags** — toggles visibility of tag blocks.
- **Open Semantic Map** — shows Mermaid graph panel.
- **Regenerate Semantic Graph** — refreshes visualization based on tag blocks.

## Metadata Model
- Tags must persist even when hidden to ensure synchronization integrity.
- UUID-based parent-child chains enable multi-layer semantic depth from note to individual terms.
- Graph generation consumes tag relationships and parent-child links.

## Development Notes
- Avoid try/catch around imports.
- Prepare interfaces for plugging into Python/Postgres sync flows (listeners, polling, or command-driven updates).
- Maintain clean separation of AI logic, tagging utilities, UI components, and settings management to support future extensibility.
