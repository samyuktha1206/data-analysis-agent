# 🧠 Claude Data Analysis Agent
An intelligent Data Analysis Agent built using Claude Agent SDK that analyzes CSV datasets, validates data quality, and generates structured insights with actionable recommendations.

## 📋 Overview
This project implements a single-agent system using Claude’s Agent SDK to handle common data analysis queries such as:

**Aggregation** → “What’s the total revenue?”\
**Top-N queries** → “Show top 5 products by revenue.”\
**Filtering** → “What’s the revenue for Product X?”

It includes two working modes:\
**🟢 Interactive Agent** — Persistent conversational session that can be resumed between runs.\
**⚡ One-Shot Agent** — Executes a single query and exits, persisting state as a JSON record.


## 🧩 Features
✅ Supports 3 core query types:

**Aggregation:** Calculates total for a numeric column\
**Top-N:** Lists top n entries sorted by a column\
**Filtering:** Filters dataset rows by column value

✅ Implements 3 built-in tools:

| Tool                                  | Description                                     |
| ------------------------------------- | ----------------------------------------------- |
| `calculate_total_tool(column)`        | Computes total of a numeric column              |
| `get_top_n_tool(column, n)`           | Returns top N rows sorted by column             |
| `filter_by_value_tool(column, value)` | Filters rows matching value                     |
| `validate_data_tool()`                | Ensures dataset is valid before running queries |


✅ Graceful error handling:

● Detects missing or invalid data\
● Asks clarifying questions for ambiguous queries\
● Provides recommendations based on insights

✅ Persistent state:

**Interactive mode** stores session_id to resume conversations.\
**One-shot mode** stores structured agent states (JSON) under state/one-shot/.

## 🏗️ Project Structure

📦 claude-data-analysis-agent/\
├── agents/\
│&ensp;&ensp;   ├── agent.py&ensp;&ensp;# Interactive agent implementation\
│&ensp;&ensp;   ├── one_shot_agent.py&ensp;&ensp;# One-shot agent with persisted state\
│
├── data/\
│&ensp;&ensp;   └── sample_data.csv&ensp;&ensp;# Dataset (~50 rows of products & revenues)\
│
├── state/\
│&ensp;&ensp;   ├── interactive/&ensp;&ensp;         # Stores session ID for interactive runs\
│&ensp;&ensp;   └── one-shot/&ensp;&ensp;            # Stores JSON state files per one-shot query\
│
├── tools.py&ensp;&ensp;                 # Tool implementations\
├── main.py&ensp;&ensp;               ```# Entry point\
├── requirements.txt&ensp;&ensp;         # Dependencies\
└── README.md


## ⚙️ Installation & Setup\
1️⃣ **Clone and install**\
git clone <your-repo-url>\
cd claude-data-analysis-agent\
pip install -r requirements.txt

2️⃣ **Configure environment**\
Create a .env file at the root:

ANTHROPIC_API_KEY=your_claude_api_key\
DATA_PATH=data/sample_data.csv

**3️⃣ Run the agent**\
**🗨️ Interactive Mode**\
python main.py\
Resume previous sessions automatically via stored session_id.

## ⚡ One-Shot Mode
python main.py "What's the total revenue?"\
Each run saves a JSON record under state/one-shot/:\
state/one-shot/agent_state_20251101T093500Z.json


## 🧠 Example Queries
| Query                             | Intent      | Tool Used              |
| --------------------------------- | ----------- | ---------------------- |
| “What’s the total revenue?”       | Aggregation | `calculate_total_tool` |
| “Show top 5 products by revenue.” | Top-N       | `get_top_n_tool`       |
| “What’s revenue for Product A?”   | Filtering   | `filter_by_value_tool` |


## 🧾 Example Output
**Human-readable summary:**

Total revenue for the dataset is 45,234.75.

**Structured output:**

{\
  "intent": "aggregation",\
  "supporting_data": {"column": "revenue", "total": 45234.75},\
  "recommendation": "Increase promotion on the top-selling product to grow revenue by focusing on repeat buyers.",\
  "reasoning": "Used calculate_total_tool on 'revenue' to sum all sales.",\
  "data_issues": []\
}

## 🧰 Dependencies

pandas>=1.5\
python-dotenv>=1.0\
jsonschema>=4.0\
claude-agent-sdk>=0.1\
typing-extensions>4.0

## 🗂️ Persistent State

**Interactive:**
state/interactive/session_id.txt — used to resume sessions.

**One-Shot:**
state/one-shot/agent_state_<timestamp>.json — stores query, result, insights, data issues.

**Example one-shot state:**

{
  "query": "Show top 3 products by revenue",\
  "intent": "top_n",\
  "results": { "column": "revenue", "n": 3, "rows": [...] },\
  "insights": "Claude recommends promoting these top performers.",\
  "data_issues": [],\
  "timestamp": "2025-11-01T09:35:00Z"
}

## 🧪 Graceful Handling

| Scenario        | Behavior                         |
| --------------- | -------------------------------- |
| Dataset missing | Returns structured error message |
| Ambiguous query | Asks clarifying question         |
| Data anomalies  | Mentions missing/negative values |
| Empty rows      | Explains insufficiency clearly   |

## 📈 Future Improvements
● Memory: Remember insights across sessions\
● Query suggestions: Suggest relevant follow-up questions

## 👩‍💻 Author
Samyuktha Deenadayalan
AI/ML & Full-Stack Developer passionate about intelligent agent systems.
