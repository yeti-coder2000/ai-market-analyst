# AI Market Analyst Agent Instructions

These instructions apply to the entire repository.

## Repository and deployment authority

- GitHub `main` is the Render production source.
- Codex is used for code, tests, diffs, commits, and pull requests.
- Work is used for briefing/chart analysis, research, and approval of changes.

## Approval rules

- Do not implement observations, suggestions, analysis notes, or ideas unless they are explicitly marked `APPROVED FIX`.
- Treat any unapproved request as research-only unless the user provides explicit approved scope.

## Git workflow

- Do not commit directly to `main`.
- Use a task or feature branch and open a pull request to `main`.
- A production hotfix exception requires explicit authorization.

## Secrets and local state

- Never read, print, commit, or modify `.env`, API keys, tokens, or production secrets.
- Do not copy production runtime artifacts into Git.

## Layer separation

- Keep Positioning Intelligence and Market Interpretation separate from each other and from main Telegram signal logic unless explicitly approved.

## Protected systems

Do not change the following areas without explicit approved scope:

- TPO/Auction core.
- TPO Watch Bridge.
- LTF detector.
- Battle Gate.
- Telegram signal delivery.
- Statistics.
- Render schedules.
- Runtime paths.

## New layer constraints

New layers start research-only and append-only. They must not:

- Mutate `battle_ready`.
- Mutate `telegram_delivery_mode`.
- Add entries, SL, TP, or executable signals.
- Overwrite TPO telemetry or Battle Gate telemetry.

## Behavior precedence and hard gates

- `current_open_behavior` always overrides initial or legacy behavior.
- Preserve the true Open Test Drive definition and all existing hard gates.

## Required pre-edit workflow

Before making edits, inspect and report:

- Applicable `AGENTS.md` files.
- `git status`.
- Affected code paths.
- Non-goals.

## Required checks

Before completion, run and report the exact checks used:

- `git diff --check`.
- `py_compile` for changed Python files.
- A focused test for changed behavior.
- A dry-run for reporting/runtime paths when those paths are affected.

If a required check is not applicable, state why.

## Operational safety

Never reset, clean runtime, restart services, or deploy without explicit authorization.

## Completion reporting

Every completion must report:

- Changed files.
- Non-goals.
- Exact checks run.
- Commit SHA if committed.
- `Render deploy: not performed` unless deployment was explicitly authorized.
