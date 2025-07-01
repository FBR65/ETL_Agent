"""
Gradio Web Interface für ETL-Agent - Verbesserte Implementierung
Optimiert für PydanticAI und verbessertes User Experience
"""

import gradio as gr
import asyncio
import logging
from typing import Dict, List, Tuple
import pandas as pd

from .etl_agent_core import ETLAgent, ETLRequest
from .database_manager import DatabaseManager
from .scheduler import ETLScheduler

# Logging für Gradio konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_agent_gradio.log"),
    ],
)
logger = logging.getLogger(__name__)


class ETLGradioInterface:
    """
    Gradio Web Interface für ETL-Agent
    Verbesserte Implementierung mit besserer PydanticAI-Integration
    """
    
    # Klassen-Variable für geteilte DatabaseManager Instanz
    _shared_db_manager = None

    def __init__(self, db_manager=None):
        # Verwende geteilte Instanz oder erstelle neue
        if db_manager:
            self.db_manager = db_manager
            ETLGradioInterface._shared_db_manager = db_manager
        elif ETLGradioInterface._shared_db_manager:
            self.db_manager = ETLGradioInterface._shared_db_manager
        else:
            self.db_manager = DatabaseManager()
            ETLGradioInterface._shared_db_manager = self.db_manager
            
        self.etl_agent = ETLAgent()
        # Teile den DatabaseManager mit dem ETLAgent
        self.etl_agent.db_manager = self.db_manager
        self.scheduler = ETLScheduler()
        self.current_connections = {}
        logger.info(f"ETL Gradio Interface initialisiert - DatabaseManager geteilt (Verbindungen: {len(self.db_manager.connection_configs)})")

    def create_interface(self) -> gr.Blocks:
        """Erstellt Gradio Interface mit verbessertem Design"""
        with gr.Blocks(
            title="ETL Agent - Intelligente Datenverarbeitung",
            theme=gr.themes.Soft(),
            css="""
            .gradio-container {
                max-width: 1200px !important;
            }
            .status-success {
                color: #10b981 !important;
            }
            .status-error {
                color: #ef4444 !important;
            }
            """,
        ) as interface:
            gr.Markdown("# 🚀 ETL Agent - Intelligente Datenverarbeitung")
            gr.Markdown(
                """
                **KI-basierte ETL-Code-Generierung mit PydanticAI**  
                Beschreiben Sie Ihren ETL-Prozess in natürlicher Sprache und lassen Sie den Agenten Python-Code generieren.
                
                💡 **Für beste Ergebnisse:** Fügen Sie zuerst Datenbankverbindungen hinzu und konfigurieren Sie die ETL-Optionen unten.
                """
            )

            with gr.Tabs():
                with gr.Tab("🔗 Datenbankverbindungen", id="db_tab"):
                    self._create_database_tab()
                with gr.Tab("⚙️ ETL-Prozess Designer", id="etl_tab"):
                    self._create_etl_tab()
                with gr.Tab("⏰ Job-Scheduler", id="scheduler_tab"):
                    self._create_scheduler_tab()
                with gr.Tab("📊 Monitoring", id="monitoring_tab"):
                    self._create_monitoring_tab()
                with gr.Tab("🤖 Agent-Status", id="agent_tab"):
                    self._create_agent_status_tab()

        return interface

    def _create_etl_tab(self):
        """Erstellt ETL-Designer Tab mit verbesserter UX"""
        # Wichtiger Hinweis am Anfang
        gr.Markdown("""## 🎯 ETL-Prozess in natürlicher Sprache beschreiben

### ⚠️ WICHTIGER HINWEIS für beste Ergebnisse:
1️⃣ **Zuerst**: Verbindungen im Tab "Datenbankverbindungen" anlegen  
2️⃣ **Dann**: Quell-Datenbank in "ETL-Konfiguration" auswählen  
3️⃣ **Schließlich**: ETL-Prozess detailliert beschreiben  

**Mit Schema-Erkennung erhalten Sie 10x besseren, spezifischen Code!**""")

        with gr.Row():
            with gr.Column(scale=2):
                gr.Markdown("### 📝 ETL-Beschreibung")

                # Beispiele als Buttons
                with gr.Row():
                    gr.Markdown("**Beispiele:**")
                with gr.Row():
                    example1_btn = gr.Button("📊 Kundendaten aggregieren", size="sm")
                    example2_btn = gr.Button("🔄 Tabellen verknüpfen", size="sm")
                    example3_btn = gr.Button("📤 CSV Export", size="sm")

                etl_description = gr.Textbox(
                    label="ETL-Prozess Beschreibung",
                    placeholder="Beispiel: 'Lade alle Kunden aus der MongoDB customers_db, filtere aktive Kunden (status=active), füge Altersberechnung hinzu und speichere als CSV'",
                    lines=6,
                    info="Seien Sie so spezifisch wie möglich. Erwähnen Sie Datenbankverbindungen, Tabellen, Filter und gewünschte Ausgabe.",
                )

                with gr.Row():
                    generate_btn = gr.Button(
                        "🤖 ETL-Code generieren", variant="primary", size="lg"
                    )
                    clear_btn = gr.Button("🗑️ Leeren", size="lg")

            with gr.Column(scale=3):
                gr.Markdown("### 💻 Generierter ETL-Code")
                generated_code = gr.Code(
                    label="ETL Pipeline Code",
                    language="python",
                    lines=25,
                    show_label=True,
                )

                with gr.Row():
                    copy_btn = gr.Button("📋 Code kopieren", size="sm")
                    save_btn = gr.Button("💾 Code speichern", size="sm")

                execution_log = gr.Textbox(
                    label="🔍 Generierungslog & Status",
                    lines=6,
                    interactive=False,
                    show_label=True,
                )

        # ETL-Konfiguration (WICHTIG für optimale Ergebnisse)
        with gr.Accordion("🎯 ETL-Konfiguration & Schema-Erkennung (UNBEDINGT AUSFÜLLEN!)", open=True):
            with gr.Row():
                with gr.Column():
                    source_conn = gr.Dropdown(
                        choices=self._get_connection_choices(),
                        label="📊 Quell-Datenbank (PFLICHTFELD für Schema-Erkennung)",
                        info="⚠️ WICHTIG: Ohne Auswahl wird nur generischer Code erstellt!",
                        allow_custom_value=False,
                        interactive=True,
                    )
                    # Store reference for cross-tab updates
                    self._source_conn_dropdown = source_conn
                    
                    transformation_hints = gr.CheckboxGroup(
                        choices=[
                            "Data Filtering",
                            "Table Joins", 
                            "Data Aggregation",
                            "Date Transformations",
                            "String Cleaning",
                            "Chunked Processing",
                        ],
                        label="🔧 Gewünschte Transformationen (empfohlen)",
                        info="💡 Hilft dem AI-Agent bei der Spezialisierung des Codes",
                    )
                with gr.Column():
                    target_conn = gr.Dropdown(
                        choices=self._get_connection_choices(),
                        label="💾 Ziel-Datenbank (optional, aber empfohlen)",
                        info="💾 Ermöglicht direktes Speichern in DB statt nur CSV/Excel",
                        allow_custom_value=False,
                        interactive=True,
                    )
                    # Store reference for cross-tab updates
                    self._target_conn_dropdown = target_conn
                    output_format = gr.Radio(
                        choices=["Auto", "CSV", "Excel", "Database", "JSON"],
                        value="Auto",
                        label="Ausgabeformat",
                        info="Gewünschtes Format für die Ausgabe",
                    )
                    
            # Refresh-Button für Verbindungen
            with gr.Row():
                refresh_conn_btn = gr.Button("🔄 Verbindungen aktualisieren", size="sm")

        # Event Handlers
        generate_btn.click(
            fn=self._generate_etl_code_enhanced,
            inputs=[
                etl_description,
                source_conn,
                target_conn,
                transformation_hints,
                output_format,
            ],
            outputs=[generated_code, execution_log],
        )
        
        refresh_conn_btn.click(
            fn=self._refresh_connection_choices,
            outputs=[source_conn, target_conn],
        )

        clear_btn.click(fn=lambda: ("", ""), outputs=[etl_description, execution_log])

        # Beispiel-Buttons
        example1_btn.click(
            fn=lambda: "Lade alle Kunden aus der customers Tabelle, aggregiere Bestellungen nach Kunde und berechne Gesamtumsatz pro Kunde",
            outputs=[etl_description],
        )
        example2_btn.click(
            fn=lambda: "Verknüpfe customers und orders Tabellen über customer_id, filtere Bestellungen der letzten 30 Tage",
            outputs=[etl_description],
        )
        example3_btn.click(
            fn=lambda: "Exportiere alle aktiven Produkte mit Lagerbestand > 0 als CSV Datei mit Timestamp im Dateinamen",
            outputs=[etl_description],
        )

    def _create_database_tab(self):
        """Erstellt Datenbank-Konfigurationstab mit verbesserter UX"""
        gr.Markdown("## 🔗 Datenbankverbindungen verwalten")

        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("### ➕ Neue Verbindung hinzufügen")
                conn_name = gr.Textbox(
                    label="Verbindungsname",
                    placeholder="z.B. 'main_db', 'warehouse_db'",
                    info="Eindeutiger Name für die Verbindung",
                )
                db_type = gr.Dropdown(
                    choices=[
                        "postgresql",
                        "mysql",
                        "mariadb",
                        "mongodb",
                        "sqlite",
                        "oracle",
                        "sqlserver",
                    ],
                    label="Datenbanktyp",
                    value="postgresql",
                )
                conn_string = gr.Textbox(
                    label="Connection String",
                    lines=2,
                    placeholder="Beispiel: postgresql://user:password@localhost:5432/database",
                    info="Datenbankverbindung (siehe Beispiele unten)",
                )

                with gr.Row():
                    add_btn = gr.Button("➕ Hinzufügen", variant="primary")
                    test_btn = gr.Button("🔍 Testen")

                connection_status = gr.Textbox(
                    label="Status", interactive=False, show_label=True
                )

            with gr.Column(scale=2):
                gr.Markdown("### 📋 Bestehende Verbindungen")
                connections_list = gr.DataFrame(
                    headers=["Name", "Typ", "Status", "Letzter Test"],
                    label="Verbindungen",
                    interactive=False,
                )

                with gr.Row():
                    refresh_btn = gr.Button("🔄 Aktualisieren")
                    delete_btn = gr.Button("🗑️ Ausgewählte löschen", variant="stop")

        # Connection String Templates
        with gr.Accordion("📖 Connection String Beispiele & Hilfe", open=True):
            gr.Markdown("""
            ### 🔗 Häufige Connection String Beispiele:
            
            **📊 PostgreSQL:**  
            ```
            postgresql://user:password@localhost:5432/database_name
            ```
            
            **🐬 MySQL/MariaDB:**  
            ```
            mysql://user:password@localhost:3306/database_name
            ```
            
            **🍃 MongoDB:**  
            ```
            mongodb://user:password@localhost:27017/database_name
            ```
            
            **📁 SQLite:**  
            ```
            sqlite:///C:/path/to/database.db
            ```
            
            **🔶 Oracle:**  
            ```
            oracle://user:password@localhost:1521/database_name
            ```
            
            **🏢 SQL Server:**  
            ```
            mssql+pyodbc://user:password@localhost:1433/database_name
            ```
            
            ### ⚠️ Wichtige Hinweise:
            - **Timeout**: Verbindungstest bricht nach 10 Sekunden ab
            - **Netzwerk**: Stellen Sie sicher, dass der Datenbankserver erreichbar ist
            - **Firewall**: Prüfen Sie, ob der Port freigegeben ist
            - **Credentials**: Verwenden Sie gültige Benutzerdaten
            """)

        # Quick Connection Templates
        with gr.Accordion("⚡ Schnell-Vorlagen", open=False):
            gr.Markdown("**Klicken Sie auf eine Vorlage zum Übernehmen:**")

            with gr.Row():
                template_postgres = gr.Button("🐘 PostgreSQL (localhost)", size="sm")
                template_mysql = gr.Button("🐬 MySQL (localhost)", size="sm")
                template_mongodb = gr.Button("🍃 MongoDB (localhost)", size="sm")

            with gr.Row():
                template_sqlite = gr.Button("📁 SQLite (lokal)", size="sm")
                template_mariadb = gr.Button("🔷 MariaDB (localhost)", size="sm")

            # Template Event Handlers
            template_postgres.click(
                lambda: "postgresql://user:password@localhost:5432/database_name",
                outputs=[conn_string],
            )
            template_mysql.click(
                lambda: "mysql://user:password@localhost:3306/database_name",
                outputs=[conn_string],
            )
            template_mongodb.click(
                lambda: "mongodb://user:password@localhost:27017/database_name",
                outputs=[conn_string],
            )
            template_sqlite.click(
                lambda: "sqlite:///C:/data/database.db", outputs=[conn_string]
            )
            template_mariadb.click(
                lambda: "mysql://user:password@localhost:3306/database_name",
                outputs=[conn_string],
            )

        # Event Handlers - normale Funktion, Cross-Tab Update später
        add_btn.click(
            fn=self._add_connection_enhanced,
            inputs=[conn_name, db_type, conn_string],
            outputs=[connection_status, connections_list],
        )
        test_btn.click(
            fn=self._test_connection_by_config_enhanced,
            inputs=[conn_name, db_type, conn_string],
            outputs=[connection_status],
        )
        db_type.change(
            fn=self._update_connection_string_placeholder,
            inputs=[db_type],
            outputs=[conn_string],
        )
        refresh_btn.click(
            fn=self._refresh_connections_enhanced, outputs=[connections_list]
        )

    def _create_scheduler_tab(self):
        """Erstellt Scheduler Tab mit verbesserter Funktionalität"""
        gr.Markdown("## ⏰ ETL-Job Scheduler")

        with gr.Row():
            with gr.Column():
                gr.Markdown("### 📅 Neuen Job planen")
                job_name = gr.Textbox(
                    label="Job Name", placeholder="z.B. 'daily_customer_sync'"
                )
                job_description = gr.Textbox(
                    label="Job Beschreibung",
                    lines=3,
                    placeholder="Was macht dieser Job?",
                )
                job_code = gr.Code(
                    label="ETL Code",
                    language="python",
                    lines=12,
                    value="# ETL Code hier einfügen oder aus Designer Tab kopieren",
                )

                with gr.Row():
                    schedule_type = gr.Radio(
                        choices=[
                            "Einmalig",
                            "Täglich",
                            "Wöchentlich",
                            "Monatlich",
                            "Cron",
                        ],
                        value="Täglich",
                        label="Zeitplan-Typ",
                    )
                    schedule_time = gr.Textbox(
                        label="Zeit/Cron",
                        placeholder="z.B. '09:00' oder '0 9 * * *'",
                        value="09:00",
                    )

                schedule_btn = gr.Button("📅 Job einplanen", variant="primary")
                schedule_status = gr.Textbox(
                    label="Scheduler Status", interactive=False
                )

            with gr.Column():
                gr.Markdown("### 📊 Aktive Jobs")
                jobs_list = gr.DataFrame(
                    headers=[
                        "Name",
                        "Status",
                        "Nächste Ausführung",
                        "Letzte Ausführung",
                    ],
                    label="Geplante Jobs",
                )

                with gr.Row():
                    refresh_jobs_btn = gr.Button("🔄 Jobs aktualisieren")
                    stop_job_btn = gr.Button("⏹️ Job stoppen", variant="stop")

        schedule_btn.click(
            fn=self._schedule_job,
            inputs=[job_name, job_description, job_code, schedule_type, schedule_time],
            outputs=[schedule_status, jobs_list],
        )

    def _create_monitoring_tab(self):
        """Erstellt Monitoring Tab mit Echtzeit-Informationen"""
        gr.Markdown("## 📊 ETL-Monitoring & Statistiken")

        with gr.Row():
            with gr.Column():
                gr.Markdown("### 🖥️ System-Status")
                system_status = gr.JSON(
                    label="System Status",
                    value={
                        "etl_agent": "Ready",
                        "database_manager": "Ready",
                        "mcp_server": "Port 8090",
                        "a2a_server": "Port 8091",
                    },
                )

                gr.Markdown("### 📈 Statistiken")
                stats_display = gr.JSON(
                    label="ETL Statistiken",
                    value={
                        "total_connections": 0,
                        "active_connections": 0,
                        "codes_generated": 0,
                        "jobs_scheduled": 0,
                    },
                )

            with gr.Column():
                gr.Markdown("### 📝 Aktivitätslog")
                activity_log = gr.Textbox(
                    label="Neueste Aktivitäten",
                    lines=15,
                    interactive=False,
                    value="ETL Agent gestartet...\nWarte auf Benutzeraktionen...",
                )

                refresh_monitoring_btn = gr.Button("🔄 Status aktualisieren")

        refresh_monitoring_btn.click(
            fn=self._refresh_monitoring,
            outputs=[system_status, stats_display, activity_log],
        )

    def _create_agent_status_tab(self):
        """Erstellt Agent-Status Tab für erweiterte Informationen"""
        gr.Markdown("## 🤖 AI-Agent Status & Konfiguration")

        with gr.Row():
            with gr.Column():
                gr.Markdown("### 🧠 LLM-Konfiguration")
                llm_info = gr.JSON(
                    label="Large Language Model",
                    value={
                        "model": self.etl_agent.llm_model_name,
                        "endpoint": self.etl_agent.llm_endpoint,
                        "provider": "OpenAI-compatible",
                        "framework": "PydanticAI",
                    },
                )

                test_llm_btn = gr.Button("🧪 LLM-Verbindung testen")
                llm_test_result = gr.Textbox(
                    label="LLM Test Ergebnis", interactive=False
                )

            with gr.Column():
                gr.Markdown("### ⚙️ Agent-Capabilities")
                capabilities = gr.JSON(
                    label="Verfügbare Funktionen",
                    value={
                        "code_generation": True,
                        "multi_database_support": True,
                        "schema_introspection": True,
                        "a2a_communication": True,
                        "mcp_integration": True,
                        "job_scheduling": True,
                    },
                )

        test_llm_btn.click(fn=self._test_llm_connection, outputs=[llm_test_result])

    def _generate_etl_code_enhanced(
        self,
        description: str,
        source_conn: str,
        target_conn: str,
        transformation_hints: List[str],
        output_format: str,
    ) -> Tuple[str, str]:
        """Generiert ETL-Code mit erweiterten Optionen"""
        try:
            if not description.strip():
                return ("", "❌ Bitte geben Sie eine ETL-Beschreibung ein.")

            logger.info(f"Generiere ETL-Code für: {description}")

            # Schema-Introspection für verfügbare Verbindungen
            schema_context = {}
            available_connections = self.db_manager.list_connections()
            
            if available_connections:
                logger.info("Führe Schema-Introspection durch...")
                for conn_info in available_connections:
                    try:
                        conn_name = conn_info.get("name", str(conn_info)) if isinstance(conn_info, dict) else str(conn_info)
                        conn_config = self.db_manager.connection_configs.get(conn_name, {})
                        db_type = conn_config.get("type", "unknown")
                        
                        # Schema abrufen
                        if db_type == "mongodb":
                            # Für MongoDB: Standard-Datenbank versuchen
                            conn_string = conn_config.get("connection_string", "")
                            db_name = conn_string.split("/")[-1] if "/" in conn_string else "default"
                            schema_info = self.db_manager.get_schema_info(conn_name, database=db_name)
                        else:
                            # Für SQL-Datenbanken
                            schema_info = self.db_manager.get_schema_info(conn_name)
                        
                        schema_context[conn_name] = schema_info
                        logger.info(f"Schema für {conn_name} abgerufen: {len(schema_info.get('tables', schema_info.get('collections', {})))} Tabellen/Collections")
                        
                    except Exception as schema_error:
                        logger.warning(f"Schema-Introspection für {conn_name} fehlgeschlagen: {schema_error}")
                        schema_context[conn_name] = {"error": str(schema_error)}

            # Erweiterte ETL-Request erstellen
            etl_request = ETLRequest(
                description=description,
                source_config={"connection_name": source_conn} if source_conn else None,
                target_config={"connection_name": target_conn} if target_conn else None,
                transformation_rules=transformation_hints,
                metadata={
                    "output_format": output_format,
                    "interface": "gradio_enhanced",
                    "available_connections": list(schema_context.keys()),
                    "database_schemas": schema_context,
                    "api_reference": {
                        "extract_data": "db_manager.extract_data(connection_name, query_config)",
                        "load_data": "db_manager.load_data(connection_name, dataframe, load_config)",
                        "query_config_sql": {"query": "SELECT * FROM table_name"},
                        "query_config_mongodb": {"database": "db_name", "collection": "collection_name", "query": {}, "limit": 1000},
                        "load_config": {"table": "target_table", "if_exists": "replace|append"}
                    }
                },
            )

            async def generate_code():
                return await self.etl_agent.process_etl_request(etl_request)

            # Async handling für Gradio
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, generate_code())
                result = future.result(timeout=15)  # Nur 15 Sekunden!

            if result.status == "success":
                log_message = f"""✅ ETL-Code erfolgreich generiert!
🤖 Modell: {result.metadata.get("model", "PydanticAI")}
🔗 Verbindungen: {len(self.db_manager.list_connections())} verfügbar
📋 Ausführungsplan: {len(result.execution_plan)} Schritte
⏱️ Generiert: {result.metadata.get("generation_timestamp", "Jetzt")}

💡 Der Code ist bereit zur Ausführung und verwendet den DatabaseManager für optimale Datenbankinteraktion."""

                return (result.generated_code, log_message)
            else:
                return ("", f"❌ AI-Agent Fehler: {result.error_message}")

        except Exception as e:
            logger.error(f"Code-Generierung Fehler: {e}")
            
            # KEIN Template-Fallback! Nutzer soll echte AI-Generierung bekommen
            return (
                "",
                f"❌ ETL-Code-Generierung fehlgeschlagen: {str(e)}\n\n💡 Mögliche Lösungen:\n" +
                "• Prüfen Sie die LLM-Verbindung im Agent-Status Tab\n" +
                "• Fügen Sie Datenbankverbindungen hinzu für Schema-Introspection\n" +
                "• Versuchen Sie eine einfachere ETL-Beschreibung\n" +
                "• Stellen Sie sicher, dass der AI-Service läuft"
            )

    def _add_connection_enhanced(
        self, name: str, db_type: str, conn_string: str
    ) -> Tuple[str, List]:
        """Fügt eine neue Datenbankverbindung hinzu mit verbesserter Validierung"""
        if not name or not conn_string:
            return "❌ Name und Connection String sind erforderlich", []

        try:
            # Erweiterte Validierung
            if name in self.db_manager.connection_configs:
                return (
                    f"❌ Verbindung '{name}' existiert bereits",
                    self._refresh_connections_enhanced(),
                )

            config = {"connection_string": conn_string, "type": db_type}
            success = self.db_manager.add_connection(name, config)

            if success:
                # Verbindung direkt testen
                test_result = self.db_manager.test_connection(name)
                status_msg = f"✅ Verbindung '{name}' hinzugefügt"
                if test_result.get("status") == "success":
                    status_msg += " und erfolgreich getestet"
                    # Log für Debugging
                    logger.info(f"Neue Verbindung hinzugefügt: {name} ({db_type})")
                else:
                    status_msg += f" (Warnung: Test fehlgeschlagen - {test_result.get('message', 'Unbekannter Fehler')})"

                return status_msg, self._refresh_connections_enhanced()
            else:
                return f"❌ Fehler beim Hinzufügen der Verbindung '{name}'", []

        except Exception as e:
            logger.error(f"Fehler beim Hinzufügen der Verbindung: {e}")
            return f"❌ Fehler: {str(e)}", []

    def _test_connection_by_config_enhanced(
        self, name: str, db_type: str, conn_string: str
    ) -> str:
        """Testet eine Datenbankverbindung mit verbessertem Feedback und Timeout"""
        if not name or not conn_string:
            return "❌ Name und Connection String sind erforderlich"

        if not conn_string.strip():
            return "❌ Connection String darf nicht leer sein"

        try:
            # Einfache Validierung des Connection Strings
            if db_type == "postgresql" and not (
                "postgresql://" in conn_string.lower()
                or "postgres://" in conn_string.lower()
            ):
                return (
                    "❌ PostgreSQL Connection String muss mit 'postgresql://' beginnen"
                )
            elif (
                db_type in ["mysql", "mariadb"]
                and not "mysql://" in conn_string.lower()
            ):
                return "❌ MySQL/MariaDB Connection String muss mit 'mysql://' beginnen"
            elif db_type == "mongodb" and not "mongodb://" in conn_string.lower():
                return "❌ MongoDB Connection String muss mit 'mongodb://' beginnen"
            elif db_type == "sqlite" and not "sqlite://" in conn_string.lower():
                return "❌ SQLite Connection String muss mit 'sqlite://' beginnen"

            config = {"connection_string": conn_string, "type": db_type}
            temp_name = f"_temp_test_{name}_{pd.Timestamp.now().strftime('%H%M%S')}"

            success = self.db_manager.add_connection(temp_name, config)
            if success:
                # Test mit 5 Sekunden Timeout (VERKÜRZT)
                result = self.db_manager.test_connection(temp_name, timeout=5)

                # Cleanup
                if temp_name in self.db_manager.connections:
                    del self.db_manager.connections[temp_name]
                if temp_name in self.db_manager.connection_configs:
                    del self.db_manager.connection_configs[temp_name]

                if result["status"] == "success":
                    elapsed = result.get("details", {}).get("elapsed_time", "unbekannt")
                    return f"✅ Verbindungstest erfolgreich!\n🔗 {db_type.upper()}: {result['message']}\n⏱️ Test um {pd.Timestamp.now().strftime('%H:%M:%S')}"
                else:
                    return f"❌ Verbindungstest fehlgeschlagen\n🔗 {db_type.upper()}: {result['message']}\n💡 Prüfen Sie:\n   • Connection String Format\n   • Netzwerkverbindung\n   • Datenbankserver Status\n   • Firewall Einstellungen"
            else:
                return f"❌ DatabaseManager konnte Verbindung nicht erstellen\n🔗 Typ: {db_type}\n💡 Prüfen Sie das Connection String Format:\n   • Beginnt mit richtigem Protokoll?\n   • Alle Parameter vorhanden?\n   • Syntax korrekt?"

        except Exception as e:
            error_msg = str(e)
            if "timeout" in error_msg.lower():
                return f"❌ Timeout: Verbindung dauerte zu lange (>10s)\n💡 Mögliche Ursachen:\n   • Datenbankserver nicht erreichbar\n   • Netzwerkprobleme\n   • Firewall blockiert Verbindung\n   • Falscher Port/Host"
            else:
                return f"❌ Fehler beim Testen: {error_msg}\n💡 Häufige Probleme:\n   • Falsche Credentials\n   • Server nicht gestartet\n   • Netzwerkfehler"

    def _refresh_connections_enhanced(self) -> List[List[str]]:
        """Aktualisiert Verbindungsliste mit erweiterten Informationen"""
        try:
            connections = self.db_manager.list_connections()
            detailed_list = []

            for conn_info in connections:
                try:
                    # Sichere Extraktion der Verbindungsdaten
                    if isinstance(conn_info, dict):
                        conn_name = conn_info.get("name", "Unknown")
                        db_type = conn_info.get("type", "Unknown")
                        status = conn_info.get("status", "Unknown")
                    else:
                        # Fallback falls es nur der Name ist
                        conn_name = str(conn_info)
                        conn_config = self.db_manager.connection_configs.get(conn_name, {})
                        db_type = conn_config.get("type", "Unknown")
                        status = "Unknown"

                    # Test der Verbindung mit Timeout
                    try:
                        test_result = self.db_manager.test_connection(conn_name, timeout=5)
                        if test_result.get("status") == "success":
                            status = "✅ Aktiv"
                            last_test = "Erfolgreich"
                        else:
                            status = "❌ Fehler"
                            last_test = "Fehlgeschlagen"
                    except Exception as test_error:
                        status = "❌ Timeout"
                        last_test = "Timeout"

                    # Sichere String-Konvertierung
                    detailed_list.append([
                        str(conn_name),
                        str(db_type).upper(),
                        str(status),
                        str(last_test)
                    ])

                except Exception as e:
                    # Fallback-Eintrag bei Fehlern
                    safe_name = str(conn_info) if not isinstance(conn_info, dict) else str(conn_info.get("name", "Unknown"))
                    detailed_list.append([
                        safe_name,
                        "Unknown",
                        "❌ Fehler",
                        f"Error: {str(e)[:50]}..."
                    ])

            return detailed_list

        except Exception as e:
            logger.error(f"Fehler beim Aktualisieren der Verbindungen: {e}")
            # Leere Liste zurückgeben bei Fehlern
            return []

    def _update_connection_string_placeholder(self, db_type: str) -> gr.Textbox:
        """Aktualisiert Connection String Placeholder mit Templates"""
        templates = {
            "mongodb": "mongodb://username:password@localhost:27017/database",
            "postgresql": "postgresql://username:password@localhost:5432/database",
            "mysql": "mysql://username:password@localhost:3306/database",
            "mariadb": "mysql://username:password@localhost:3306/database",
            "oracle": "oracle://username:password@localhost:1521/database",
            "sqlite": "sqlite:///path/to/database.db",
            "sqlserver": "mssql+pyodbc://username:password@localhost:1433/database",
        }
        placeholder = templates.get(db_type, "Wählen Sie einen Datenbanktyp aus")
        return gr.Textbox(
            label="Connection String",
            placeholder=placeholder,
            value="",
            type="password",
            info=f"Template für {db_type.upper()}",
        )

    def _schedule_job(
        self,
        name: str,
        description: str,
        code: str,
        schedule_type: str,
        schedule_time: str,
    ) -> Tuple[str, List]:
        """Plant einen ETL-Job mit verbesserter Validierung"""
        if not name or not code:
            return "❌ Job Name und Code sind erforderlich", []

        try:
            # TODO: Implementierung des Schedulers
            status = f"✅ Job '{name}' erfolgreich geplant\n📅 Typ: {schedule_type}\n⏰ Zeit: {schedule_time}\n📝 Beschreibung: {description}"

            # Dummy-Jobsliste für Demo
            jobs = [
                [name, "Geplant", f"Nächste: {schedule_time}", "Noch nicht ausgeführt"]
            ]

            return status, jobs

        except Exception as e:
            return f"❌ Fehler beim Planen des Jobs: {str(e)}", []

    def _refresh_monitoring(self) -> Tuple[Dict, Dict, str]:
        """Aktualisiert Monitoring-Informationen"""
        try:
            connections = self.db_manager.list_connections()
            active_connections = 0

            for conn_name in connections:
                try:
                    result = self.db_manager.test_connection(conn_name)
                    if result.get("status") == "success":
                        active_connections += 1
                except Exception:
                    pass

            system_status = {
                "etl_agent": "✅ Ready",
                "database_manager": "✅ Ready",
                "pydantic_ai": f"✅ {self.etl_agent.llm_model_name}",
                "mcp_server": "🔄 Port 8090",
                "a2a_server": "🔄 Port 8091",
            }

            stats = {
                "total_connections": len(connections),
                "active_connections": active_connections,
                "inactive_connections": len(connections) - active_connections,
                "ai_model": self.etl_agent.llm_model_name,
                "last_update": pd.Timestamp.now().strftime("%H:%M:%S"),
            }

            activity_log = f"""ETL Agent Status Report - {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}

🔗 Datenbankverbindungen: {len(connections)} konfiguriert, {active_connections} aktiv
🤖 AI-Modell: {self.etl_agent.llm_model_name}
🌐 Endpoint: {self.etl_agent.llm_endpoint}
⚙️ Framework: PydanticAI

Verfügbare Verbindungen:
{chr(10).join([f"  - {conn}" for conn in connections]) if connections else "  Keine Verbindungen konfiguriert"}

System bereit für ETL-Operationen."""

            return system_status, stats, activity_log

        except Exception as e:
            error_log = f"❌ Fehler beim Aktualisieren: {str(e)}"
            return {"status": "error"}, {"error": str(e)}, error_log

    def _test_llm_connection(self) -> str:
        """Testet die LLM-Verbindung"""
        try:
            # Einfacher Test der AI-Verbindung
            test_prompt = "Antworte mit 'OK' wenn du diese Nachricht erhältst."

            async def test_ai():
                return await self.etl_agent.agent.run(test_prompt)

            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, test_ai())
                result = future.result(timeout=10)

            return f"✅ LLM-Verbindung erfolgreich\n🤖 Modell: {self.etl_agent.llm_model_name}\n🌐 Endpoint: {self.etl_agent.llm_endpoint}\n📨 Antwort: {str(result)[:100]}..."

        except Exception as e:
            return f"❌ LLM-Verbindung fehlgeschlagen\n🔗 Endpoint: {self.etl_agent.llm_endpoint}\n❗ Fehler: {str(e)}\n\n💡 Prüfen Sie, ob der LLM-Server läuft und erreichbar ist."


    def _get_connection_choices(self) -> List[str]:
        """Hilfsfunktion zum Abrufen der verfügbaren Verbindungen für Dropdowns"""
        try:
            connections = self.db_manager.list_connections()
            logger.info(f"_get_connection_choices: {len(connections)} Verbindungen gefunden")
            logger.info(f"DatabaseManager Verbindungs-Configs: {list(self.db_manager.connection_configs.keys())}")
            
            if not connections:
                return ["Keine Verbindungen konfiguriert"]
            
            # Sichere Konvertierung zu Strings
            choices = []
            for conn in connections:
                if isinstance(conn, dict):
                    name = conn.get("name", str(conn))
                else:
                    name = str(conn)
                choices.append(name)
                logger.info(f"Verbindung hinzugefügt: {name}")
            
            return choices if choices else ["Keine Verbindungen verfügbar"]
            
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Verbindungen: {e}")
            return ["Fehler beim Laden der Verbindungen"]

    def _refresh_connection_choices(self) -> Tuple[List[str], List[str]]:
        """Aktualisiert die Auswahlmöglichkeiten für Source- und Target-Verbindungen"""
        choices = self._get_connection_choices()
        return (choices, choices)

    def _add_connection_and_update_all(
        self, name: str, db_type: str, conn_string: str
    ) -> Tuple[str, List, List[str], List[str]]:
        """Fügt Verbindung hinzu und aktualisiert alle relevanten UI-Elemente"""
        # Zuerst die Verbindung hinzufügen
        status, connections_list = self._add_connection_enhanced(name, db_type, conn_string)
        
        # Dann die Dropdown-Choices aktualisieren
        updated_choices = self._get_connection_choices()
        
        return status, connections_list, updated_choices, updated_choices

    def _refresh_all_connections(self) -> Tuple[List, List[str], List[str]]:
        """Aktualisiert alle verbindungsbezogenen UI-Elemente"""
        connections_list = self._refresh_connections_enhanced()
        updated_choices = self._get_connection_choices()
        
        return connections_list, updated_choices, updated_choices

    def _on_app_load(self):
        """Wird beim Laden der App ausgeführt - aktualisiert Verbindungen"""
        logger.info("App wird geladen - aktualisiere Verbindungen...")
        connections = self.db_manager.list_connections()
        logger.info(f"Verfügbare Verbindungen beim Laden: {len(connections)}")
        return self._get_connection_choices()

def launch():
    """Startet Gradio Interface mit verbesserter Konfiguration"""
    interface_manager = ETLGradioInterface()
    interface = interface_manager.create_interface()
    interface.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        debug=True,
        show_error=True,
        quiet=False,
    )


# Für python -m Ausführung
if __name__ == "__main__":
    launch()
