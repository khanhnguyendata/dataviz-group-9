# dataviz-group-9
KU Leuven Data Visualization 2026 Group 9 project

## Links

- **Group report:** [Exploration Project Report](https://docs.google.com/document/d/1r4OXU1-8bp_lBqYTbt8uTu2IiSzEwpVz7FwY0dxQqNY/edit?usp=sharing)
- **Dashboards walkthrough (YouTube):** [video](https://youtu.be/eG6vIQ8lVGQ?si=ECrUfpkgwXmBFXb4)

### Dashboards (Marimo)

- **Erik Lambrechts** — Visualization 1: Spatial and temporal visualization of board members’ trips — [open dashboard](https://molab.marimo.io/github/khanhnguyendata/dataviz-group-9/blob/main/erik_marimo.py/wasm?show-code=false)
- **Dao Phung** — Visualization 2: Average sentiments by discussion category and board member/topic — [open dashboard](https://molab.marimo.io/github/khanhnguyendata/dataviz-group-9/blob/main/dao_marimo.py/wasm?show-code=false)
- **Khanh Nguyen** — Visualization 3: Schema diagram with indicators of data discrepancies — [open dashboard](https://molab.marimo.io/github/khanhnguyendata/dataviz-group-9/blob/main/khanh_marimo.py/wasm?show-code=false)

## Marimo

Install dependencies (includes [marimo](https://marimo.io)):

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Run a notebook app locally (from the repository root, with the virtual environment activated):

```bash
marimo run dao_marimo.py
marimo run erik_marimo.py
marimo run khanh_marimo.py
```