# Farm Expense Command Center – Ingestion Engine

PDF invoice ingestion pipeline: extract text from PDFs, parse with OpenAI into structured JSON, validate, and print. Version 1 — no UI, no database, no web framework.

## Setup

1. **Clone / open the project** and create a virtual environment (recommended):

   ```bash
   python -m venv venv
   venv\Scripts\activate   # Windows
   # source venv/bin/activate  # macOS/Linux
   ```

2. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment:**

   - Copy `.env.example` to `.env`
   - Set your OpenAI API key in `.env`:
     ```
     OPENAI_API_KEY=sk-proj-your-actual-key-here
     ```
   - Do not commit `.env` (it is in `.gitignore`).

4. **Add a sample invoice:**

   - Place a PDF invoice at `invoices/sample_invoice.pdf`.
   - The pipeline is tuned for PG&E-style utility invoices but works with other vendors.

## Usage

From the project root:

```bash
python main.py
```

Expected flow:

1. **Extracting PDF...** – Converts `invoices/sample_invoice.pdf` to markdown (pymupdf4llm).
2. **Sending to LLM...** – Sends markdown to OpenAI (gpt-4o) and receives structured JSON.
3. **Validating output...** – Checks required fields and types.
4. **Structured Output:** – Prints the validated JSON to the terminal.

## Output schema

The pipeline produces JSON in this shape:

- `vendor_name`, `invoice_number`, `total_amount` — required (non-null).
- `invoice_date`, `due_date`, `service_address`, `account_number` — optional (may be `null`).
- `line_items` — array of `{ "description": "string", "amount": float }` (may be empty).

Missing fields are set to `null`; the LLM does not guess values.

## Project layout

- `config.py` – Loads `.env` and validates `OPENAI_API_KEY`.
- `extractor.py` – PDF → markdown via pymupdf4llm.
- `llm_parser.py` – Markdown → JSON via OpenAI gpt-4o.
- `validator.py` – Validates required fields and types.
- `main.py` – Runs the pipeline and prints JSON.
- `invoices/` – Place `sample_invoice.pdf` here.

## Error handling

- **File not found** – Ensure `invoices/sample_invoice.pdf` exists.
- **Empty PDF** – Pipeline exits with a clear message.
- **OpenAI errors** – Network or API errors are caught and reported.
- **Invalid JSON from LLM** – Raises a clear parse error.
- **Validation failure** – Describes which required field or type failed.

## Scope (this version)

- No database, web framework, or UI.
- No batch processing, auth, or analytics.
- Single PDF path; modular and reusable for other vendor types.
