# DurableFlow

An engineering exercise: a durable executor for an AI agent's planned tool calls. No model runs here; the plans are given. Read `durableflow_brief.pdf` first; this file only says how to run and submit.

## Contents

- `durableflow/model.py`, `clock.py`, `gateway.py`, `__init__.py`: the types, the clock and the remote gateway. Do not modify them.  
- `durableflow/engine.py`: the supplied engine, a first version that handles the happy path. You may replace it.  
- `tests/test_contract.py`: contract tests. Some fail against the supplied engine. Put your own tests in new files, `tests/test_*.py`.  
- `run_demo.py`: a short run with a lost acknowledgment, a crash and a restart; its output shows what the supplied engine gets wrong.  
- `logs/`: three weeks of logs from payflow, the system this replaces: its engine's events, the gateway's record of every call, and the on-call notes.

## Run

```
python run_demo.py
python -m unittest discover -s tests -v
```

Python 3.11 or later, standard library only.

## Submit

One zip of the repository folder, with your engine, any tests or fault harnesses you wrote, and `SOLUTION_NOTES.md` (at most one page, about 600 words). We advise you to add your AI assistant or coding agent sessions in `transcripts/`.

&nbsp;

&nbsp;