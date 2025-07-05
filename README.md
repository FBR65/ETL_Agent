# 🚀 ETL Agent - Intelligente Datenverarbeitung

Ein KI-basierter ETL-Agent mit PydanticAI, A2A (Agent-to-Agent) Communication und MCP (Model Context Protocol) Integration.

## 🎯 Hauptfunktionalitäten

### 1. **AI-basierte ETL-Code-Generierung**
- **Natürlichsprachige Eingabe**: Beschreiben Sie Ihren ETL-Prozess in normaler Sprache
- **Automatische Code-Generierung**: PydanticAI mit Qwen2.5 LLM generiert funktionsfähigen Python-Code
- **Multi-Database Support**: Unterstützt MySQL, MariaDB, PostgreSQL, MongoDB, SQLite, Oracle, SQL Server
- **Intelligente Code-Bereinigung**: Automatische Syntaxfehler-Korrektur und Formatierung

**Beispiel:**
```
Eingabe: "Lade Daten aus Emil DB und addiere 1 zum Alter, exportiere als CSV"
Ausgabe: Vollständiger Python ETL-Code mit DatabaseManager-Integration
```

### 2. **Multi-Database Management**
- **Flexible Datenbankverbindungen**: Konfiguration verschiedener Datenbank-Typen
- **Connection String Auto-Korrektur**: Automatische Anpassung für verschiedene DB-Dialekte
- **Schema-Introspection**: Automatisches Auslesen von Tabellenstrukturen
- **Connection Testing**: Integrierte Verbindungstests mit detailliertem Feedback

### 3. **Gradio Web Interface**
- **ETL-Prozess Designer**: Intuitive Benutzeroberfläche für ETL-Beschreibungen
- **Fresh Connections System**: Dropdowns laden automatisch die neuesten Verbindungen bei jedem Klick
- **Zero Event-Chain Complexity**: Einfacher, wartbarer Code ohne komplexe Cross-Tab-Updates
- **Instant Synchronization**: Alle Tabs zeigen immer aktuelle Verbindungen
- **Non-blocking UI**: Asynchrone Operationen verhindern UI-Hängen
- **Real-time Feedback**: Sofortige Statusmeldungen bei allen Operationen

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

```
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   Gradio UI     │ │   MCP Server    │ │   A2A Server    │
│   Port: 7860    │ │   Port: 8090    │ │   Port: 8091    │
└─────────────────┘ └─────────────────┘ └─────────────────┘
        │                   │                   │
        └─────────────────────┼─────────────────────┘
                              │
        ┌─────────────────────▼─────────────────────┐
        │           ETL Agent Core                  │
        │        (PydanticAI + LLM)                │
        └─────────────────────┬─────────────────────┘
                              │
        ┌─────────────────────▼─────────────────────┐
        │         Database Manager                  │
        │      (Multi-DB Connectors)               │
        └───────────────────────────────────────────┘
```

### Komponenten
- **ETL Agent Core**: Kern-Engine mit PydanticAI-Integration
- **Database Manager**: Multi-Database Abstraktionsschicht mit Fresh Connection Loading
- **Gradio Interface**: Web-basierte Benutzeroberfläche mit revolutionärem UI-System
- **MCP Server**: Model Context Protocol Server
- **A2A Server**: Agent-to-Agent Communication Server
- **Scheduler Service**: Job-Scheduling und -Management

## 🚀 Installation & Start

### 1. Dependencies installieren
```bash
uv sync
```

### 2. Alle Services starten
```bash
python launcher.py
```

### 3. Services einzeln starten
```bash
# Gradio Web UI
python -m etl_agent.gradio_interface

# MCP Server
python -m etl_agent.mcp_server

# A2A Server
python -m etl_agent.agent_to_a2a
```

## 📱 Zugriff auf die Services

- **Gradio Web UI**: http://localhost:7860
- **MCP Server**: http://localhost:8090
- **A2A Server**: http://localhost:8091

## 💾 Datenbankunterstützung

### Unterstützte Datenbanken
- **MySQL/MariaDB**: `mysql://user:pass@host:port/db`
- **PostgreSQL**: `postgresql://user:pass@host:port/db`
- **MongoDB**: `mongodb://user:pass@host:port/db`
- **SQLite**: `sqlite:///path/to/database.db`
- **Oracle**: `oracle://user:pass@host:port/db`
- **SQL Server**: `mssql+pyodbc://user:pass@host:port/db`

### Connection Management
- **Instant Save**: Verbindungen werden sofort gespeichert ohne UI-Blockierung
- **Fresh Dropdown System**: Automatisches Laden aktueller Verbindungen bei Dropdown-Focus
- **Smart UI Updates**: Keine komplexen Event-Chains - einfacher, robuster Ansatz
- **Cross-Tab Synchronization**: ETL-Designer und Database-Tabs sind immer synchronisiert
- **Connection Testing**: Asynchrone Verbindungstests ohne UI-Hängen
- **Real-time Feedback**: Sofortige Statusmeldungen bei allen Operationen

## 🤖 AI-Integration

### LLM-Konfiguration
- **Standard**: Qwen2.5 über Ollama (localhost:11434)
- **Anpassbar**: Jeder OpenAI-kompatible Endpoint
- **PydanticAI**: Strukturierte AI-Responses mit Datenvalidierung
- **Retry-Mechanismus**: Automatische Wiederholung bei Fehlern

### Prompt Engineering
- **Spezialisierte Prompts**: Optimiert für ETL-Code-Generierung
- **Context-aware Generierung**: Basierend auf verfügbaren Datenbanken
- **Automatische Code-Optimierung**: Syntaxfehler-Korrektur und Formatierung
- **Fresh Connection Awareness**: Berücksichtigung aktueller Datenbankverbindungen

## 📊 Monitoring & Logging

### Service Status
- **Real-time Dashboard**: Live-Übersicht aller Services
- **Health Checks**: Automatische Service-Überwachung
- **Performance Metrics**: Response-Zeiten und Durchsatz

### Log-Dateien
- **Gradio Logs**: `etl_agent_gradio.log` mit UTF-8-Encoding
- **Service Logs**: Über uvicorn/fastapi
- **ETL Execution Logs**: In generierten Scripten

## ✨ Revolutionary UI/UX Features (v2.0)

### Fresh Connections System
- **On-Demand Loading**: Dropdowns laden Verbindungen nur bei Bedarf (Focus-Event)
- **Zero Event-Chain Complexity**: Einfacher, wartbarer Code ohne komplexe Cross-Tab-Updates
- **Instant Synchronization**: Alle Tabs zeigen immer aktuelle Daten
- **Memory Efficient**: Keine unnötigen Speicher-Overhead durch Event-Listener

### Performance Optimierungen
- **Async UI Operations**: Alle langwierigen Operationen sind asynchron
- **Direct JSON Access**: Ultra-schnelle Datenabfragen ohne ORM-Overhead
- **Smart Caching**: Intelligente Zwischenspeicherung für bessere Performance
- **Non-blocking Interface**: UI bleibt immer responsiv

### User Experience
- **Instant Feedback**: Sofortige Statusmeldungen bei allen Aktionen
- **Progressive Enhancement**: Interface funktioniert auch bei langsamen Verbindungen
- **Error Resilience**: Robuste Fehlerbehandlung mit benutzerfreundlichen Meldungen
- **Consistent State**: Alle UI-Komponenten sind immer synchronisiert

## 🔧 Erweiterte Funktionen

### Code-Generierung Features
- **Chunking Support**: Automatische Optimierung für große Datasets
- **Error Handling**: Robuste Fehlerbehandlung in generiertem Code
- **Schema-aware**: Berücksichtigung von Datenbankstrukturen
- **Performance Optimization**: Automatische Query-Optimierung

### Multi-Agent Capabilities
- **Agent Discovery**: Automatische Erkennung verfügbarer Agents
- **Task Distribution**: Verteilung komplexer ETL-Tasks auf mehrere Agents
- **Result Aggregation**: Zusammenführung von Multi-Agent-Ergebnissen

## 📝 Beispiel-Workflows

1. **Einfacher ETL-Prozess**: Datenextraktion, Transformation und Export
2. **Komplexer Transformations-Prozess**: Multi-Table-Joins mit Aggregationen
3. **Multi-Agent Koordination**: Verteilte Verarbeitung großer Datasets

## 🛠️ Technische Details

### Architektur-Prinzipien
- **Microservices**: Lose gekoppelte Service-Architektur
- **API-First**: REST-basierte Kommunikation zwischen Services
- **Plugin-System**: Erweiterbare Connector-Architektur
- **Configuration-driven**: Flexible Konfiguration über Environment Variables

### Performance-Optimierungen
- **Connection Pooling**: Effiziente Datenbankverbindungen
- **Async Processing**: Non-blocking I/O für bessere Performance
- **Chunked Processing**: Memory-effiziente Verarbeitung großer Datasets
- **Intelligent Caching**: Schema-Informationen und Connection-Status
- **Fresh Connection Loading**: On-Demand Loading verhindert unnötige Ressourcenverbrauch

## 🔒 Sicherheit & Best Practices

### Datenbankverbindungen
- **Sichere Speicherung**: Verschlüsselte Connection Strings
- **Verbindungsvalidierung**: Automatische Tests vor Verwendung
- **Input-Sanitization**: Schutz vor SQL-Injection

### Code-Generierung
- **Sandbox-Execution**: Sichere Ausführung generierten Codes
- **Input-Validierung**: Strikte Eingabevalidierung
- **Error Handling**: Robuste Fehlerbehandlung

## 🚀 Neue Features (v2.0)

### ✅ Fresh Connections System
- **Revolutionäres UI-Design**: Keine komplexen Event-Chains mehr
- **Instant Synchronization**: Alle Dropdowns immer aktuell
- **Zero Configuration**: Funktioniert out-of-the-box
- **Performance**: Ultra-schnelle JSON-basierte Updates
- **Robustheit**: Einfacher, wartbarer Code

### ✅ Async-First Architecture
- **Non-blocking UI**: Interface hängt nie mehr
- **Progressive Enhancement**: Bessere User Experience
- **Error Resilience**: Graceful Degradation bei Fehlern
- **Real-time Feedback**: Sofortige Statusupdates

### ✅ Production-Ready Features
- **UTF-8 Logging**: Korrekte Behandlung von Unicode-Zeichen
- **Instant UI Updates**: Sofortige Dropdown-Aktualisierung nach Änderungen
- **Robust Error Handling**: Graceful Degradation bei Netzwerkproblemen
- **Memory Efficient**: Optimierter Ressourcenverbrauch

## 🎉 Fazit

Der ETL Agent v2.0 stellt einen Meilenstein in der Entwicklung von KI-basierten Datenverarbeitungstools dar. Mit dem revolutionären Fresh Connections System, der async-first Architektur und der nahtlosen Multi-Database-Integration bietet er eine beispiellose Kombination aus Benutzerfreundlichkeit, Performance und Robustheit.

**Perfekt für:**
- Data Engineers, die schnelle ETL-Prototypen benötigen
- Entwickler, die komplexe Datenverarbeitungspipelines erstellen
- Teams, die KI-gestützte Automatisierung einsetzen möchten
- Organisationen, die Multi-Database-Umgebungen verwalten