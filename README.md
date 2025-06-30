# 🚀 ETL Agent - Intelligente Datenverarbeitung

Ein KI-basierter ETL-Agent mit PydanticAI, A2A (Agent-to-Agent) Communication und MCP (Model Context Protocol) Integration.

## 🎯 Hauptfunktionalitäten

### 1. **AI-basierte ETL-Code-Generierung**
- **Natürlichsprachige Eingabe**: Beschreiben Sie Ihren ETL-Prozess in normaler Sprache
- **Automatische Code-Generierung**: PydanticAI mit Qwen2.5 LLM generiert funktionsfähigen Python-Code
- **Multi-Database Support**: Unterstützt MySQL, MariaDB, PostgreSQL, MongoDB, SQLite, Oracle, SQL Server
- **Intelligente Code-Bereinigung**: Automatische Syntaxfehler-Korrektur und Formatierung

**Beispiel:**
Eingabe: "Lade Daten aus Emil DB und addiere 1 zum Alter, exportiere als CSV" Ausgabe: Vollständiger Python ETL-Code mit DatabaseManager-Integration


### 2. **Multi-Database Management**
- **Flexible Datenbankverbindungen**: Konfiguration verschiedener Datenbank-Typen
- **Connection String Auto-Korrektur**: Automatische Anpassung für verschiedene DB-Dialekte
- **Schema-Introspection**: Automatisches Auslesen von Tabellenstrukturen
- **Connection Testing**: Integrierte Verbindungstests mit detailliertem Feedback

### 3. **Gradio Web Interface**
- **ETL-Prozess Designer**: Intuitive Benutzeroberfläche für ETL-Beschreibungen
- **Datenbankverbindungen verwalten**: GUI für Connection-Management
- **Job-Scheduler**: Planen und verwalten von ETL-Jobs
- **Real-time Monitoring**: Live-Status und Logs aller Services

### 4. **Agent-to-Agent (A2A) Communication**
- **FastAPI-basierter A2A Server**: Port 8091
- **Standardisierte Agent-Kommunikation**: REST API für Inter-Agent-Communication
- **Capabilities Discovery**: Automatische Erkennung verfügbarer Agent-Funktionen
- **Multi-Agent Orchestration**: Koordination verschiedener AI-Agents

### 5. **Model Context Protocol (MCP) Integration**
- **MCP Server**: Port 8090
- **Standardisierte KI-Modell-Kommunikation**: Einheitliche Schnittstelle für LLMs
- **Context Management**: Intelligente Kontext-Verwaltung für bessere AI-Antworten
- **Tool Integration**: Nahtlose Integration von AI-Tools und -Funktionen

### 6. **ETL Job Scheduler**
- **Zeitbasierte Ausführung**: Einmalig, täglich, wöchentlich, monatlich
- **Job-Management**: Erstellen, bearbeiten, löschen von ETL-Jobs
- **Monitoring Dashboard**: Übersicht über alle geplanten und ausgeführten Jobs
- **Fehlerbehandlung**: Automatische Retry-Mechanismen und Alerting

## 🏗️ Systemarchitektur

### Services

┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐ │ Gradio UI │ │ MCP Server │ │ A2A Server │ │ Port: 7860 │ │ Port: 8090 │ │ Port: 8091 │ └─────────────────┘ └─────────────────┘ └─────────────────┘ │ │ │ └─────────────────────┼─────────────────────┘ │ ┌─────────────────────▼─────────────────────┐ │ ETL Agent Core │ │ (PydanticAI + LLM) │ └─────────────────────┬─────────────────────┘ │ ┌─────────────────────▼─────────────────────┐ │ Database Manager │ │ (Multi-DB Connectors) │ └───────────────────────────────────────────┘


### Komponenten
- **ETL Agent Core**: Kern-Engine mit PydanticAI-Integration
- **Database Manager**: Multi-Database Abstraktionsschicht
- **Gradio Interface**: Web-basierte Benutzeroberfläche
- **MCP Server**: Model Context Protocol Server
- **A2A Server**: Agent-to-Agent Communication Server
- **Scheduler Service**: Job-Scheduling und -Management

## 🚀 Installation & Start

### 1. Dependencies installieren
```bash
uv sync
```
2. Umgebungsvariablen konfigurieren
3. Alle Services starten
4. Services einzeln starten
📱 Zugriff auf die Services
Gradio Web UI: http://localhost:7860
MCP Server: http://localhost:8090
A2A Server: http://localhost:8091
Scheduler API: http://localhost:8092
💾 Datenbankunterstützung
Unterstützte Datenbanken
MySQL/MariaDB: mysql://user:pass@host:port/db
PostgreSQL: postgresql://user:pass@host:port/db
MongoDB: mongodb://user:pass@host:port/db
SQLite: sqlite:///path/to/database.db
Oracle: oracle://user:pass@host:port/db
SQL Server: mssql+pyodbc://user:pass@host:port/db
Connection Management
Verbindung über Gradio UI hinzufügen
Connection String eingeben
Verbindung testen
In ETL-Prozessen verwenden
🤖 AI-Integration
LLM-Konfiguration
Standard: Qwen2.5 über Ollama (localhost:11434)
Anpassbar: Jeder OpenAI-kompatible Endpoint
PydanticAI: Strukturierte AI-Responses mit Datenvalidierung
Retry-Mechanismus: Automatische Wiederholung bei Fehlern
Prompt Engineering
Spezialisierte Prompts für ETL-Code-Generierung
Context-aware Generierung basierend auf verfügbaren Datenbanken
Automatische Code-Optimierung und -Bereinigung
📊 Monitoring & Logging
Service Status
Log-Dateien
Gradio Logs: etl_agent_gradio.log
Service Logs: Über uvicorn/fastapi
ETL Execution Logs: In generierten Scripten
🔧 Erweiterte Funktionen
Code-Generierung Features
Chunking Support: Automatische Optimierung für große Datasets
Error Handling: Robuste Fehlerbehandlung in generiertem Code
Schema-aware: Berücksichtigung von Datenbankstrukturen
Performance Optimization: Automatische Query-Optimierung
Multi-Agent Capabilities
Agent Discovery: Automatische Erkennung verfügbarer Agents
Task Distribution: Verteilung komplexer ETL-Tasks auf mehrere Agents
Result Aggregation: Zusammenführung von Multi-Agent-Ergebnissen
📝 Beispiel-Workflows
1. Einfacher ETL-Prozess
2. Komplexer Transformations-Prozess
3. Multi-Agent Koordination
🛠️ Technische Details
Architektur-Prinzipien
Microservices: Lose gekoppelte Service-Architektur
API-First: REST-basierte Kommunikation zwischen Services
Plugin-System: Erweiterbare Connector-Architektur
Configuration-driven: Flexible Konfiguration über Environment Variables
Performance-Optimierungen
Connection Pooling: Effiziente Datenbankverbindungen
Async Processing: Non-blocking I/O für bessere Performance
Chunked Processing: Memory-effiziente Verarbeitung großer Datasets
Caching: Intelligentes Caching von Schema-Informationen
🔒 Sicherheit & Best Practices
Datenbankverbindungen
Sichere Speicherung von Connection Strings
Verschlüsselung sensibler Daten
Verbindungsvalidierung vor Verwendung
Code-Generierung
Sandbox-Execution für generierten Code
SQL-Injection-Schutz
Input-Validierung und -Sanitization
📈 Roadmap
Geplante Features
<input disabled="" type="checkbox"> Grafische ETL-Pipeline Designer
<input disabled="" type="checkbox"> Integration mit Cloud-Plattformen (AWS, Azure, GCP)
<input disabled="" type="checkbox"> Advanced Monitoring & Alerting
<input disabled="" type="checkbox"> Machine Learning Pipeline Integration
<input disabled="" type="checkbox"> Real-time Stream Processing
<input disabled="" type="checkb