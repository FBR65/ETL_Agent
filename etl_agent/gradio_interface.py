"""
Gradio Web Interface für ETL-Agent
"""

import gradio as gr
import asyncio
import json
import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

from .etl_agent_core import ETLAgent, ETLRequest
from .database_manager import DatabaseManager
from .scheduler import ETLScheduler

# Logging für Gradio konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Console output
        logging.FileHandler("etl_agent_gradio.log"),  # File output
    ],
)
logger = logging.getLogger(__name__)


class ETLGradioInterface:
    """Gradio Web Interface für ETL-Agent"""

    def __init__(self):
        self.etl_agent = ETLAgent()
        self.db_manager = DatabaseManager()
        self.scheduler = ETLScheduler()
        self.current_connections = {}

    def create_interface(self) -> gr.Blocks:
        """Erstellt Gradio Interface"""
        with gr.Blocks(
            title="ETL Agent - Intelligente Datenverarbeitung", theme=gr.themes.Soft()
        ) as interface:
            gr.Markdown("# 🚀 ETL Agent - Intelligente Datenverarbeitung")
            gr.Markdown(
                "Beschreiben Sie Ihren ETL-Prozess in natürlicher Sprache und lassen Sie den Agenten Python-Code generieren."
            )

            with gr.Tabs():
                # Tab 1: ETL-Prozess Designer
                with gr.Tab("⚙️ ETL-Prozess Designer"):
                    self._create_etl_tab()

                # Tab 2: Datenbankverbindungen
                with gr.Tab("🔗 Datenbankverbindungen"):
                    self._create_database_tab()

                # Tab 3: Job-Scheduler
                with gr.Tab("⏰ Job-Scheduler"):
                    self._create_scheduler_tab()

                # Tab 4: Monitoring
                with gr.Tab("📊 Monitoring"):
                    self._create_monitoring_tab()

        return interface

    def _create_etl_tab(self):
        """Erstellt ETL-Designer Tab"""
        gr.Markdown("## ETL-Prozess in natürlicher Sprache beschreiben")

        with gr.Row():
            with gr.Column(scale=2):
                etl_description = gr.Textbox(
                    label="ETL-Prozess Beschreibung",
                    placeholder="""Beispiele:
- 'Lade Daten von MongoDB Collection users, filtere aktive Benutzer und speichere in PostgreSQL'
- 'Transformiere CSV-Datei: bereinige Emails, konvertiere Datumsformat'
- 'Führe JOIN zwischen Kunden und Bestellungen durch, aggregiere nach Monat'""",
                    lines=5,
                )

                generate_btn = gr.Button(
                    "ETL-Code generieren", variant="primary", size="lg"
                )

            with gr.Column(scale=3):
                gr.Markdown("### Generierter ETL-Code")
                generated_code = gr.Code(
                    label="ETL Pipeline Code", language="python", lines=20
                )

                execution_log = gr.Textbox(
                    label="Ausführungslog", lines=5, interactive=False
                )

        # Event Handler
        generate_btn.click(
            fn=self._generate_etl_code,
            inputs=[etl_description],
            outputs=[generated_code, execution_log],
        )

    def _create_database_tab(self):
        """Erstellt Datenbank-Konfigurationstab"""
        gr.Markdown("## Datenbankverbindungen verwalten")

        with gr.Row():
            with gr.Column(scale=1):
                gr.Markdown("### Neue Verbindung hinzufügen")

                conn_name = gr.Textbox(
                    label="Verbindungsname",
                    placeholder="z.B. 'source_db' oder 'target_dwh'",
                )

                db_type = gr.Dropdown(
                    choices=[
                        "mongodb",
                        "postgresql",
                        "mysql",
                        "mariadb",
                        "oracle",
                        "sqlite",
                        "sqlserver",
                    ],
                    label="Datenbanktyp",
                    value="postgresql",
                )

                conn_string = gr.Textbox(
                    label="Connection String",
                    placeholder="z.B. postgresql://user:password@localhost:5432/database",
                )

                add_btn = gr.Button("Verbindung hinzufügen", variant="primary")
                test_btn = gr.Button("Verbindung testen")

                connection_status = gr.Textbox(label="Status", interactive=False)

            with gr.Column(scale=1):
                gr.Markdown("### Bestehende Verbindungen")
                connections_list = gr.DataFrame(
                    headers=["Name", "Typ", "Status"], label="Verbindungen"
                )

                refresh_btn = gr.Button("Aktualisieren")

        # Event Handlers
        add_btn.click(
            fn=self._add_connection,
            inputs=[conn_name, db_type, conn_string],
            outputs=[connection_status, connections_list],
        )

        test_btn.click(
            fn=self._test_connection_by_config,
            inputs=[conn_name, db_type, conn_string],
            outputs=[connection_status],
        )

        # Event Handler für DB-Typ Änderung
        db_type.change(
            fn=self._update_connection_string_placeholder,
            inputs=[db_type],
            outputs=[conn_string],
        )

        refresh_btn.click(fn=self._refresh_connections, outputs=[connections_list])

    def _create_scheduler_tab(self):
        """Erstellt Scheduler Tab"""
        gr.Markdown("## ETL-Job Scheduler")

        with gr.Row():
            with gr.Column():
                gr.Markdown("### Neuen Job planen")
                job_name = gr.Textbox(label="Job Name")
                job_code = gr.Code(label="ETL Code", language="python", lines=10)
                schedule_btn = gr.Button("Job einplanen", variant="primary")

            with gr.Column():
                gr.Markdown("### Aktive Jobs")
                jobs_list = gr.DataFrame(
                    headers=["Name", "Status"], label="Geplante Jobs"
                )

    def _create_monitoring_tab(self):
        """Erstellt Monitoring Tab"""
        gr.Markdown("## ETL-Monitoring & Statistiken")

        with gr.Row():
            with gr.Column():
                gr.Markdown("### System-Status")
                system_status = gr.JSON(label="System Status")

    def _add_connection(
        self, name: str, db_type: str, conn_string: str
    ) -> Tuple[str, List]:
        """Fügt eine neue Datenbankverbindung hinzu"""
        if not name or not conn_string:
            return "❌ Name und Connection String sind erforderlich", []

        try:
            config = {"connection_string": conn_string, "type": db_type}
            success = self.db_manager.add_connection(name, config)

            if success:
                connections = self.db_manager.list_connections()
                return f"✅ Verbindung '{name}' erfolgreich hinzugefügt", connections
            else:
                return f"❌ Fehler beim Hinzufügen der Verbindung '{name}'", []
        except Exception as e:
            return f"❌ Fehler: {str(e)}", []

    def _test_connection_by_config(
        self, name: str, db_type: str, conn_string: str
    ) -> str:
        """Testet eine Datenbankverbindung basierend auf den eingegebenen Konfigurationsdaten"""
        if not name or not conn_string:
            return "❌ Name und Connection String sind erforderlich"

        # Debug-Ausgaben direkt in die Gradio-Antwort
        debug_info = []

        try:
            # Temporäre Verbindung für Test erstellen
            config = {"connection_string": conn_string, "type": db_type}
            debug_info.append(f"🔍 Debug: Teste {name}, Typ: {db_type}")
            debug_info.append(f"🔍 Debug: Config erstellt: {config}")

            print(f"GRADIO DEBUG: Teste Verbindung {name} mit Typ {db_type}")
            logger.info(f"Teste Verbindung: {name}, {db_type}, {conn_string[:30]}...")

            # Test-Verbindung zum DatabaseManager hinzufügen
            temp_name = f"_temp_test_{name}"

            try:
                print(
                    f"GRADIO DEBUG: Rufe add_connection auf mit temp_name={temp_name}"
                )
                success = self.db_manager.add_connection(temp_name, config)
                print(f"GRADIO DEBUG: add_connection Ergebnis: {success}")
                debug_info.append(f"🔍 Debug: add_connection Ergebnis: {success}")
                logger.info(f"add_connection Ergebnis: {success}")
            except Exception as add_error:
                print(f"GRADIO DEBUG: Exception in add_connection: {add_error}")
                debug_info.append(f"🔍 Debug: Exception: {str(add_error)}")
                logger.error(f"add_connection Exception: {add_error}", exc_info=True)
                return (
                    f"❌ Fehler beim Erstellen der Test-Verbindung: {str(add_error)}\n"
                    f"Debug-Info:\n" + "\n".join(debug_info) + "\n"
                    f"Mögliche Ursachen:\n"
                    f"- Ungültiger Connection String\n"
                    f"- Nicht unterstützter Datenbanktyp: {db_type}\n"
                    f"- Fehlende Dependencies für {db_type}"
                )

            if success:
                debug_info.append(
                    "🔍 Debug: Verbindung erfolgreich erstellt, teste jetzt..."
                )
                try:
                    # Verbindung testen
                    result = self.db_manager.test_connection(temp_name)

                    # Temporäre Verbindung wieder entfernen
                    if temp_name in self.db_manager.connections:
                        del self.db_manager.connections[temp_name]
                    if temp_name in self.db_manager.connection_configs:
                        del self.db_manager.connection_configs[temp_name]

                    if result["status"] == "success":
                        return f"✅ Verbindungstest erfolgreich: {result['message']}"
                    else:
                        return (
                            f"❌ Verbindungstest fehlgeschlagen: {result['message']}\n"
                            f"Connection String: {conn_string[:50]}...\n"
                            f"Prüfen Sie:\n"
                            f"- Server erreichbar?\n"
                            f"- Credentials korrekt?\n"
                            f"- Port geöffnet?\n"
                            f"- Datenbank existiert?"
                        )

                except Exception as test_error:
                    # Cleanup bei Test-Fehler
                    if temp_name in self.db_manager.connections:
                        del self.db_manager.connections[temp_name]
                    if temp_name in self.db_manager.connection_configs:
                        del self.db_manager.connection_configs[temp_name]

                    return (
                        f"❌ Fehler beim Verbindungstest: {str(test_error)}\n"
                        f"Details: {type(test_error).__name__}\n"
                        f"Verbindung: {db_type} -> {conn_string.split('@')[-1] if '@' in conn_string else conn_string}"
                    )
            else:
                debug_info.append("🔍 Debug: add_connection gab False zurück")
                return (
                    f"❌ DatabaseManager konnte Verbindung nicht erstellen\n"
                    f"Debug-Info:\n" + "\n".join(debug_info) + "\n"
                    f"Datenbanktyp: {db_type}\n"
                    f"Connection String Format: {conn_string.split('://')[0] if '://' in conn_string else 'Unbekannt'}\n"
                    f"Unterstützte Typen: mongodb, postgresql, mysql, mariadb, sqlite"
                )

        except Exception as e:
            debug_info.append(f"🔍 Debug: Outer Exception: {str(e)}")
            print(f"GRADIO DEBUG: Outer Exception: {e}")
            return (
                f"❌ Unerwarteter Fehler beim Testen: {str(e)}\n"
                f"Debug-Info:\n" + "\n".join(debug_info) + "\n"
                f"Fehlertyp: {type(e).__name__}\n"
                f"Eingaben: Name='{name}', Typ='{db_type}'\n"
                f"Connection String: {conn_string[:30]}..."
            )

    def _refresh_connections(self) -> List:
        """Aktualisiert Verbindungsliste"""
        return self.db_manager.list_connections()

    def _update_connection_string_placeholder(self, db_type: str) -> gr.Textbox:
        """Aktualisiert Connection String Placeholder basierend auf DB-Typ"""
        connection_templates = {
            "mongodb": "mongodb://username:password@localhost:27017/database",
            "postgresql": "postgresql://username:password@localhost:5432/database",
            "mysql": "mysql://username:password@localhost:3306/database",
            "mariadb": "mysql://username:password@localhost:3306/database",  # Wird automatisch zu mysql+mysqlconnector://
            "oracle": "oracle://username:password@localhost:1521/database",
            "sqlite": "sqlite:///path/to/database.db",
            "sqlserver": "mssql+pyodbc://username:password@localhost:1433/database?driver=ODBC+Driver+17+for+SQL+Server",
        }

        placeholder = connection_templates.get(
            db_type, "Wählen Sie einen Datenbanktyp aus"
        )

        return gr.Textbox(
            label="Connection String",
            placeholder=placeholder,
            value="",
        )

    def _generate_etl_code(self, description: str) -> Tuple[str, str]:
        """Generiert ETL-Code basierend auf Beschreibung"""
        try:
            code = f'''
import pandas as pd
import logging
from database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def etl_pipeline():
    """
    ETL Pipeline: {description}
    """
    logger.info("Starte ETL-Pipeline")
    
    try:
        # 1. Extract
        # TODO: Implementiere Datenextraktion
        
        # 2. Transform  
        # TODO: Implementiere Transformationslogik
        
        # 3. Load
        # TODO: Implementiere Datenladung
        
        logger.info("ETL-Pipeline erfolgreich abgeschlossen")
        
    except Exception as e:
        logger.error(f"ETL-Pipeline Fehler: {{e}}")
        raise

if __name__ == "__main__":
    etl_pipeline()
'''

            return code, "✅ ETL-Code erfolgreich generiert (Demo-Version)"

        except Exception as e:
            logger.error(f"Code-Generierung Fehler: {e}")
            return "", f"❌ Fehler bei Code-Generierung: {str(e)}"


def launch():
    """Startet Gradio Interface"""
    interface_manager = ETLGradioInterface()
    interface = interface_manager.create_interface()

    interface.launch(
        server_name="0.0.0.0",
        server_port=7860,
        share=False,
        debug=True,
    )


if __name__ == "__main__":
    launch()
