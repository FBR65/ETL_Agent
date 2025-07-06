# ETL Agent

An intelligent ETL agent with PydanticAI, Agent-to-Agent (A2A) communication and multi-database support.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Usage](#usage)
- [Services](#services)
- [Database Support](#database-support)
- [Development](#development)
- [License](#license)

## 🎯 Overview

The ETL Agent is a modern, AI-powered ETL platform that converts natural language descriptions into functional ETL code. The system combines PydanticAI for structured AI interactions, Agent-to-Agent communication for service integration, and comprehensive multi-database support.

## ✨ Features

### 🤖 AI-Powered ETL Generation
- **PydanticAI Integration**: Structured AI interactions with type-safe responses
- **Natural Language Input**: Describe ETL processes in natural language
- **Code Generation**: Automatic creation of production-ready Python ETL code
- **Intelligent Schema Detection**: Automatic analysis of database schemas

### 🔗 Multi-Database Support
- **MongoDB**: Complete NoSQL support with aggregation pipelines
- **PostgreSQL**: Advanced SQL features and JSON support
- **MySQL/MariaDB**: Optimized connectors for MySQL-based systems
- **Oracle**: Enterprise database support
- **SQL Server**: Microsoft SQL Server integration
- **SQLite**: Lightweight database support

### 🌐 Service Architecture
- **Agent-to-Agent (A2A)**: RESTful API for service integration
- **Model Context Protocol (MCP)**: Advanced context management tools
- **Gradio Web UI**: User-friendly web interface
- **ETL Scheduler**: Automated job execution

### 🛠️ Enterprise Features
- **Persistent Connections**: Secure storage of database connections
- **Async Processing**: Non-blocking ETL operations
- **Monitoring**: Comprehensive logging and metrics
- **Error Handling**: Robust error handling and recovery

## 🏗️ Architecture

```
ETL-Agent/
├── etl_agent/
│   ├── __init__.py
│   ├── etl_agent_core.py          # Core Agent with PydanticAI
│   ├── database_manager.py        # Multi-Database Manager
│   ├── agent_to_a2a.py           # A2A Communication Server
│   ├── mcp_server.py             # MCP Server
│   ├── gradio_interface.py       # Web UI
│   ├── scheduler.py              # ETL Job Scheduler
│   ├── connectors/
│   │   ├── mongodb_connector.py   # MongoDB Connector
│   │   ├── sql_connector.py       # SQL Database Connector
│   │   └── oracle_connector.py    # Oracle Connector
│   └── utils/
│       └── logger.py             # Enhanced Logging
├── tools/                        # Additional Tools
├── launcher.py                   # Service Manager
├── pyproject.toml               # UV Package Configuration
└── db_connections.json          # Persistent Connections
```

## 📦 Installation

This project uses [uv](https://docs.astral.sh/uv/) as package manager for fast and reliable dependency management.

### Prerequisites

- Python 3.10 or higher
- uv Package Manager

### uv Installation

```bash
# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Project Setup

```bash
# Clone repository
git clone <repository-url>
cd ETL_Agent

# Install dependencies
uv sync

# Install development dependencies
uv sync --dev
```

## 🚀 Quick Start

### 1. Start All Services

```bash
# Start all services with one command
uv run python launcher.py
```

This starts:
- **Gradio Web UI**: http://localhost:7860
- **MCP Server**: http://localhost:8090
- **A2A Server**: http://localhost:8091

### 2. Start Individual Services

```bash
# Web UI only
uv run python -m etl_agent.gradio_interface

# MCP Server only
uv run uvicorn etl_agent.mcp_server:app --host 0.0.0.0 --port 8090

# A2A Server only
uv run uvicorn etl_agent.agent_to_a2a:app --host 0.0.0.0 --port 8091
```

### 3. First Steps

1. **Open Web UI**: http://localhost:7860
2. **Add Database Connection**:
   ```
   Name: my_db
   Type: postgresql
   Connection String: postgresql://user:password@localhost:5432/database
   ```
3. **Generate ETL Code**:
   ```
   Description: "Copy all user data from PostgreSQL to MongoDB"
   Source Connection: my_db
   Target Connection: mongo_db
   ```

## ⚙️ Configuration

### Environment Variables

Create a `.env` file in the project directory:

```env
# OpenAI-compatible API (for code generation)
OPENAI_API_KEY=your_api_key_here
OPENAI_API_BASE=https://api.openai.com/v1
LLM_MODEL_NAME=gpt-4

# Alternative: Local LLM endpoints
# OPENAI_API_BASE=http://localhost:1234/v1
# LLM_MODEL_NAME=qwen2.5

# Database Defaults
DEFAULT_DB_CONFIG_FILE=db_connections.json
```

### Database Connections

Connections are stored in `db_connections.json`:

```json
{
  "postgresql_example": {
    "type": "postgresql",
    "connection_string": "postgresql://user:password@localhost:5432/database",
    "description": "Example PostgreSQL connection"
  },
  "mongodb_example": {
    "type": "mongodb",
    "connection_string": "mongodb://localhost:27017/",
    "description": "Example MongoDB connection"
  }
}
```

## 📘 Usage

### Web Interface

The Gradio Web UI provides the following features:

1. **ETL Generation**:
   - Enter natural language descriptions
   - Select source and target connections
   - Generate and execute ETL code

2. **Database Management**:
   - Add new connections
   - Test connections
   - Display schema information

3. **Job Scheduling**:
   - Schedule ETL jobs
   - Monitor and manage jobs

### Programmatic Usage

```python
from etl_agent.etl_agent_core import ETLAgent, ETLRequest

# Initialize ETL Agent
agent = ETLAgent()

# Create ETL request
request = ETLRequest(
    description="Copy all customers from PostgreSQL to MongoDB",
    source_config={"connection_name": "postgres_db"},
    target_config={"connection_name": "mongo_db"}
)

# Generate ETL code
response = await agent.process_etl_request(request)
print(response.generated_code)
```

### API Usage

#### A2A Server (Port 8091)

```bash
# Get capabilities
curl http://localhost:8091/a2a/capabilities

# Generate ETL code
curl -X POST http://localhost:8091/a2a/execute \
  -H "Content-Type: application/json" \
  -d '{
    "action": "generate_etl",
    "payload": {
      "description": "Copy data from MySQL to MongoDB"
    }
  }'
```

#### MCP Server (Port 8090)

```bash
# Available tools
curl http://localhost:8090/tools

# Execute tool
curl -X POST http://localhost:8090/mcp/call-tool \
  -H "Content-Type: application/json" \
  -d '{
    "name": "list_database_connections",
    "arguments": {}
  }'
```

## 🔧 Services

### 1. Gradio Web UI (Port 7860)
- User-friendly web interface
- ETL code generation
- Database management
- Job scheduling

### 2. MCP Server (Port 8090)
- Model Context Protocol implementation
- Advanced tool functions
- Schema management
- Data extraction

### 3. A2A Server (Port 8091)
- Agent-to-Agent communication
- RESTful API for service integration
- PydanticAI integration
- Structured responses

## 💾 Database Support

### Connection String Examples

```python
# PostgreSQL
"postgresql://user:password@localhost:5432/database"

# MySQL/MariaDB
"mysql+mysqlconnector://user:password@localhost:3306/database"

# MongoDB
"mongodb://user:password@localhost:27017/database"

# Oracle
"oracle://user:password@localhost:1521/xe"

# SQL Server
"mssql+pyodbc://user:password@localhost:1433/database?driver=ODBC+Driver+17+for+SQL+Server"

# SQLite
"sqlite:///path/to/database.db"
```

### Supported Operations

- **Schema Detection**: Automatic analysis of tables and fields
- **Data Extraction**: Flexible query execution
- **Data Transformation**: Pandas-based data processing
- **Data Loading**: Optimized bulk insert operations

## 🛠️ Development

### Development Environment

```bash
# Install development dependencies
uv sync --dev

# Run tests
uv run pytest

# Format code
uv run black .
uv run isort .

# Type checking
uv run mypy etl_agent/
```

### New Database Connectors

1. Create connector in `etl_agent/connectors/`
2. Register in `database_manager.py`
3. Add connection string format
4. Write tests

### Debugging

```bash
# Individual services with debug logging
uv run python -m etl_agent.gradio_interface --debug

# View logs
tail -f etl_agent_gradio.log
tail -f etl_agent_launcher.log
```

## 📊 Monitoring

### Logs

- `etl_agent_gradio.log`: Web UI logs
- `etl_agent_launcher.log`: Service manager logs
- `etl_enhanced_terminal.log`: Terminal operations

### Metrics

- Service status via health endpoints
- Database connection status
- ETL job performance

## 🤝 Contributing

1. Fork the repository
2. Create feature branch: `git checkout -b feature/new-feature`
3. Commit changes: `git commit -am 'Add new feature'`
4. Push branch: `git push origin feature/new-feature`
5. Create pull request

## 🐛 Troubleshooting

### Common Issues

1. **Port already in use**:
   ```bash
   # Kill processes on port
   uv run python launcher.py --kill-ports
   ```

2. **Database connection failed**:
   - Check connection string
   - Verify database server status
   - Check firewall settings

3. **Import errors**:
   ```bash
   # Reinstall dependencies
   uv sync --reinstall
   ```

## 📜 License

This project is licensed under the GNU Affero General Public License v3.0. See [LICENSE.md](LICENSE.md) for details.

AGPL v3.0 ensures that:
- Source code remains available to all users
- Modifications to the software must also remain open source
- Network-based services must provide the source code

## 👥 Authors

- Developed with ❤️ for the open source community
- Based on modern Python frameworks and AI technologies

## 🙏 Acknowledgments

- [PydanticAI](https://github.com/pydantic/pydantic-ai) for structured AI interactions
- [Gradio](https://gradio.app/) for the web UI
- [FastAPI](https://fastapi.tiangolo.com/) for API implementation
- [UV](https://docs.astral.sh/uv/) for package management

