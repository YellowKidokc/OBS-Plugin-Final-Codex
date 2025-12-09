# Obsidian Semantic AI Plugin (Final Master Specification)

## Overview
**Working name:** `obsidian-semantic-ai` (based on [`obsidian-note-definitions`](https://github.com/dominiclet/obsidian-note-definitions))

This document is the definitive developer brief for building a semantic Obsidian plugin plus its Python/Postgres backend. It merges inline tagging, multi-layer UUID chains, native AI classifiers, customizable prompts, batch processing, Mermaid visuals, and sync readiness into one blueprint.

---

## Part 1 — Obsidian Plugin (Front-End)

### Core Purposes
1. **Tagging**
   - User- and AI-driven tagging for Axioms, Claims, Evidence Bundles, Word Ontology, Scientific Process, Relationships, Internal/External/Forward Links, Proper Names, Sentences, and Paragraphs.
   - UUID assignment for notes, paragraphs, sentences, and words/terms with hard-encoded blocks:
     ```
     %%tag::TYPE::UUID::"Label"::parentUUID%%
     ```
   - `parentUUID` chains nested semantic depth (word → sentence → paragraph → note).

2. **Hidden Semantic Layer**
   - Metadata is appended invisibly at the bottom of notes (hidden by default).
   - Right-click/command: **“Show Hidden Semantic Layer”** toggles visibility while preserving the hard-coded blocks for sync.

### AI Integration (Native to Plugin)
- Right-click AI actions (note/selection/paragraph): **Classify This Note**, **Classify This Paragraph**, **Classify Selected Text**, **Identify Relationships**, **Generate Evidence Bundles**, **Generate Forward/Back Links**, **Build Mermaid Diagram**.
- AI runs locally or via user-provided API key; output is parsed and written as tag blocks even when hidden.

#### Prompt Templates (User Editable)
- Settings tabs per type: Axioms, Claims, Evidence, Relationships, Ontology, and more.
- Each tab stores a default prompt, is user-editable, and includes **Reset to default**.

#### Custom Classifiers
- Dedicated tab for keyword/pattern triggers with user-authored mini-prompts; auto-runs when keywords appear and emits tag blocks.

### Batch Processing
- Right-click → **Batch Process Folder**.
- Plugin counts Markdown files, estimates token cost, asks for confirmation, then processes sequentially.
- Results stream to a right-hand Results Panel, e.g.:
  ```
  Processing file: consciousness.md
  Found: 4 Axioms, 2 Claims, 3 Evidence Bundles
  ```

### Visual Story Layer (Mermaid Integration)
- Auto-generate diagrams for paper flows, Axiom → Claim → Evidence chains, relationship maps, and nested UUID hierarchies (Paragraph → Sentence → Word).
- User command/right-click: **Show Semantic Map** or **Regenerate Semantic Graph** to view in a right sidebar panel or append to the note.

### Right Sidebar (Display Panel)
- Tabs:
  - **📄 Semantic Layers:** hierarchical view (Paragraph → Sentence → Term) with UUIDs.
  - **🧠 AI Result Viewer:** shows proposed insertions before writing.
  - **🕸️ Mermaid Graph:** renders diagrams.
  - **🧷 UUID Inspector:** lists UUIDs tied to the note.

### Right-Click Commands (Notes & Folders)
- **Classify Note**, **Classify Folder**, **Rerun classifier with updated prompt**, **Show/Hide hidden semantic layer**, **Show Semantic Map**, **Generate Evidence Bundle**, **Generate Forward Links**, **Assign UUID Now**.

### Commands & Visibility
- **Run AI Classifier** (note/paragraph/selection), **Batch Classify Folder**, **Show Hidden Semantic Layer**, **Open Semantic Map**, **Regenerate Semantic Graph**.
- Tags always persist even when hidden to support sync integrity.

### Suggested File Structure
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

### Default Prompt Samples
- **Axiom:** Identify core foundational truths in this document. These are axioms — statements that do not rely on prior proof and support other claims.
- **Claim:** Identify any claims made by the author. A claim asserts a position that can be supported or refuted.
- **Evidence:** Identify evidence used to support claims or axioms. This may be empirical data, quotes, or logical arguments.
- **Relationship:** Identify explicit or implicit relationships between concepts, entities, or events in the text.
- **Word Ontology:** Identify specialized terms and link them to their definitions, origins, or ontological categories.

### Development Notes
- Avoid try/catch around imports.
- Maintain clean separation of AI logic, tagging utilities, UI components, and settings management.

---

## Part 2 — Python Backend (Heavy Lifting)
- **Deep Classification:** advanced pipelines, multi-agent/multi-document reasoning.
- **Postgres Sync:** global knowledge base for notes, tags, UUIDs, semantic maps, relationships, embeddings, web scrapes, definitions, evidence bundles.
- **UUID Master Generator:** registry to prevent collisions and resolve duplicates.
- **Semantic Integrity Checker:** detects note changes, rewrites tag blocks, updates Postgres.
- **RAG Layer:** chunking, embeddings, contextual retrieval to improve classification and evidence matching.

## Part 3 — Data & Sync Architecture
- **Source of truth:** Postgres.
- **Optional cache:** SQLite for Python side.
- **Workflow:** Obsidian writes metadata blocks → Python scans vault (interval, file-change, or on-demand) → syncs to Postgres → plugin retrieves updates as needed.

## Part 4 — Data Acquisition Layer
- **Web Scraper:** downloads HTML/PDF/text, extracts semantic data, auto-tags.
- **Definition Acquisition:** learns from glossaries/dictionaries/wikis/user-defined files.
- **CSV/Excel Importer:** converts structured rows into notes/tag blocks with UUIDs.
- **Master Sheets:** backend generates aggregated files (Axioms, Evidence, Claims, Ontologies, Relationships, Papers).

## Part 5 — System Outline
1. **Obsidian Plugin:** Tagging Engine, UUID Generator, AI Classifier, Batch Processor, Prompt System, Mermaid Visualizer, Right Sidebar Viewer, Hidden Semantic Layer, Right-click Menus.
2. **Python Backend:** Deep AI Engine, Web Scraper, Data Importer, Semantic Integrity Checker, Global UUID Registry, RAG Embedding System, Sync Manager (Vault ↔ Postgres).
3. **Postgres Database:** tables for Notes, Tags, UUID Registry, Embeddings, Relationships, Web Imports, Prompts, Custom Classifiers.
4. **Sync Architecture:** Obsidian writes metadata → Python detects updates → Postgres stores everything → plugin retrieves on demand.

## Part 6 — Open Decisions
- Should UUIDs be backend-only, plugin-only, or hybrid? (Hybrid acceptable.)
- How often should Python scan the vault? (Interval, file-change watch, or plugin-request.)

---

## Metadata Model & Graph Generation
- Tags persist even when hidden; UUID parent-child chains enable note → paragraph → sentence → term depth and relationship graphs.
- Mermaid diagrams consume tag relationships and hierarchical links to show flows (e.g., paper chains and evidence stacks).

---

## Quick Reference: Right-Click Actions
- Notes: **Classify Note**, **Show/Hide Hidden Semantic Layer**, **Open Semantic Map**, **Generate Evidence Bundle**, **Generate Forward Links**, **Rerun classifier with updated prompt**.
- Folders: **Batch Classify This Folder** with cost estimation and progress panel.

---

## Additional Implementation Guidance
- Prepare extension points for Python/Postgres listeners, polling, or command-driven updates.
- Keep UI/AI/tagging code modular to support future extensibility and backend integration.
