# Repository Guidelines

## Project Structure & Module Organization

This is a deliberately single-file Python agent. Put all runtime behavior in
`agent.py`; do not split it into new modules. `config.example.json` documents
user-configurable settings, while the untracked `config.json` holds local
credentials and paths. `run.sh` (macOS/Linux) and `run.ps1` (Windows) create
the virtual environment, install dependencies, and launch the agent.

## Build, Test, and Development Commands

- `./run.sh` creates `.venv` when needed and starts the agent on macOS/Linux.
- `./run.sh a` through `./run.sh e` select a development user and clear the
  device token before starting.
- `./run.ps1` provisions the Windows `venv` and starts the agent.
- `python3 -m pip install -r requirements.txt` installs the sole external
  dependency for manual runs.
- `python3 agent.py` runs directly after configuration and dependency setup.
- `python3 -m pytest test_agent.py -v` runs tests when `test_agent.py` exists.

Copy `config.example.json` to `config.json` before running. Never commit
`config.json`, device tokens, or real local video paths.

## Coding Style & Naming Conventions

Target Python 3.10+ and preserve cross-platform support for Windows, macOS,
and Linux. Use four-space indentation, standard-library-first imports, type
annotations for new non-trivial interfaces, and `snake_case` for functions and
variables; use `UPPER_SNAKE_CASE` for constants. Keep async WebSocket work in
the existing lifecycle and run blocking file operations through the executor.
Avoid new dependencies: only the standard library and `websockets>=12.0` are
supported. Validate untrusted paths with the existing safety helpers.

## Testing Guidelines

The repository currently has no committed test suite. Add focused `pytest`
coverage in `test_agent.py`, naming tests `test_<behavior>`. Mock network,
filesystem, and subprocess boundaries; do not require real video files,
ffprobe, or cloud access. Cover both success and failure/rollback paths for
file operations.

## Commit & Pull Request Guidelines

Use Conventional Commit-style messages seen in history, such as
`fix(agent): handle invalid tokens` or `feat(sync): report metadata`. Keep
changes narrow. Pull requests should explain the behavior change, list tested
commands, flag configuration or protocol effects, and include logs/screenshots
only when they clarify user-visible behavior. Never expose tokens or private
paths in descriptions or logs.
