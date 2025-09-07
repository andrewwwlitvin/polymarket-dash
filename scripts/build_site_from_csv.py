name: Build and publish dashboards

on:
  workflow_dispatch:
  schedule:
    - cron: "0 */6 * * *"  # every 6 hours, UTC

env:
  TZ: Europe/Lisbon
  PYTHON_VERSION: "3.11"
  REUSE_MINUTES: 0

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repo
        uses: actions/checkout@v4
        with:
          fetch-depth: 0  # we need history for rebase

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          if [ -f requirements.txt ]; then
            pip install -r requirements.txt
          fi

      # Optional: fetch latest CSVs (if your fetcher exists)
      - name: Fetch latest markets (optional)
        run: |
          set -e
          mkdir -p data
          if [ -f scripts/polymarket_enriched_fast.py ]; then
            echo "[fetch] Running fetcher..."
            python scripts/polymarket_enriched_fast.py --topk 120 --concurrency 8
            latest=$(ls -1t polymarket_enriched_fast_*.csv | head -n1 || true)
            if [ -n "$latest" ] && [ -f "$latest" ]; then
              echo "[fetch] Moving $latest -> data/"
              mv "$latest" "data/$latest"
            fi
          else
            echo "[fetch] Skipping; fetcher not present."
          fi

      - name: Select newest CSV
        id: pickcsv
        run: |
          set -e
          mkdir -p data
          echo "==== list data/ ===="
          ls -lAh data || true

          # pick newest CSV strictly under data/
          newest=$(ls -1t data/*.csv 2>/dev/null | head -n1 || true)
          if [ -z "$newest" ]; then
            echo "No CSV in data/. The builder will write a sample."
            newest=""
          fi
          echo "LATEST_CSV=$newest" >> $GITHUB_ENV
          echo "Picked CSV: ${newest:-<none>}"

      - name: Build site from CSV
        run: |
          set -e
          echo "Using LATEST_CSV: ${LATEST_CSV:-<empty>}"
          # If empty, builder falls back to sample; otherwise pass it explicitly
          if [ -n "${LATEST_CSV:-}" ]; then
            python scripts/build_site_from_csv.py "$LATEST_CSV"
          else
            python scripts/build_site_from_csv.py
          fi

          echo "---- list site/ after build ----"
          ls -lAh site || true

          echo "---- index.html head ----"
          head -n 25 site/index.html | sed 's/^/[index] /' || true

          echo "---- archive.html head ----"
          head -n 25 site/archive.html | sed 's/^/[arch ] /' || true

          echo "---- robots.txt ----"
          sed -n '1,50p' site/robots.txt || true

          echo "---- sitemap.xml head ----"
          head -n 30 site/sitemap.xml || true

      - name: Git status before staging
        run: |
          echo "==== git status (before add) ===="
          git status
          echo "==== diff vs HEAD (site/ & data/) ===="
          git diff --name-only HEAD -- site/ data/ || true

      - name: Commit and push site + data
        run: |
          set -e

          # Stage all changes (including untracked snapshot & CSV)
          git add -A site data

          echo "==== git status (after add) ===="
          git status

          # Only commit if there is something staged
          if git diff --cached --quiet ; then
            echo "Nothing to commit (no staged changes)."
            exit 0
          fi

          git config user.name  "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

          # Avoid push rejects if remote moved
          git fetch origin main
          git pull --rebase origin main

          ts="$(date -u +'%Y-%m-%d %H:%M:%S')"
          git commit -m "Auto-build: pages from latest CSV @ ${ts} UTC"
          git push origin main

      - name: Final summary
        run: |
          echo "Build completed. If the previous step pushed to main, Vercel will deploy the new HTML."