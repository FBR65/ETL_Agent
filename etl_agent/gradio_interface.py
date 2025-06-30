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
        logging.StreamHandler(),
        logging.FileHandler("etl_agent_gradio.log"),
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
                with gr.Tab("⚙️ ETL-Prozess Designer"):
                    self._create_etl_tab()
                with gr.Tab("🔗 Datenbankverbindungen"):
                    self._create_database_tab()
                with gr.Tab("⏰ Job-Scheduler"):
                    self._create_scheduler_tab()
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
                    placeholder="Beispiel: 'Lade Daten aus MongoDB, transformiere Alter und speichere in CSV'",
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
                    label="Verbindungsname", placeholder="z.B. 'source_db'"
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
                conn_string = gr.Textbox(label="Connection String")
                add_btn = gr.Button("Verbindung hinzufügen", variant="primary")
                test_btn = gr.Button("Verbindung testen")
                connection_status = gr.Textbox(label="Status", interactive=False)

            with gr.Column(scale=1):
                gr.Markdown("### Bestehende Verbindungen")
                connections_list = gr.DataFrame(
                    headers=["Name", "Typ", "Status"], label="Verbindungen"
                )
                refresh_btn = gr.Button("Aktualisieren")

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
        """Testet eine Datenbankverbindung"""
        if not name or not conn_string:
            return "❌ Name und Connection String sind erforderlich"

        try:
            config = {"connection_string": conn_string, "type": db_type}
            temp_name = f"_temp_test_{name}"

            success = self.db_manager.add_connection(temp_name, config)
            if success:
                result = self.db_manager.test_connection(temp_name)
                # Cleanup
                if temp_name in self.db_manager.connections:
                    del self.db_manager.connections[temp_name]
                if temp_name in self.db_manager.connection_configs:
                    del self.db_manager.connection_configs[temp_name]

                if result["status"] == "success":
                    return f"✅ Verbindungstest erfolgreich: {result['message']}"
                else:
                    return f"❌ Verbindungstest fehlgeschlagen: {result['message']}"
            else:
                return f"❌ DatabaseManager konnte Verbindung nicht erstellen"
        except Exception as e:
            return f"❌ Fehler beim Testen: {str(e)}"

    def _refresh_connections(self) -> List:
        """Aktualisiert Verbindungsliste"""
        return self.db_manager.list_connections()

    def _update_connection_string_placeholder(self, db_type: str) -> gr.Textbox:
        """Aktualisiert Connection String Placeholder"""
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
        return gr.Textbox(label="Connection String", placeholder=placeholder, value="")

    def _generate_etl_code(self, description: str) -> Tuple[str, str]:
        """Generiert ETL-Code mit PydanticAI Agent"""
        try:
            logger.info(f"Generiere ETL-Code für: {description}")

            etl_request = ETLRequest(
                description=description,
                source_config=None,
                target_config=None,
                transformation_rules=None,
            )

            async def generate_code():
                return await self.etl_agent.process_etl_request(etl_request)

            # Async handling für Gradio
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(asyncio.run, generate_code())
                result = future.result(timeout=30)

            if result.status == "success":
                return (
                    result.generated_code,
                    f"✅ ETL-Code mit AI-Agent generiert!\n🤖 Modell: {result.metadata.get('model', 'PydanticAI')}",
                )
            else:
                return ("", f"❌ AI-Agent Fehler: {result.error_message}")

        except Exception as e:
            logger.error(f"Code-Generierung Fehler: {e}")

            # Fallback Template
            code = f'''
import pandas as pd
import logging
from etl_agent.database_manager import DatabaseManager

logger = logging.getLogger(__name__)

def etl_pipeline():
    """ETL Pipeline: {description}"""
    logger.info("Starte ETL-Pipeline")
    
    try:
        db_manager = DatabaseManager()
        
        # 1. Extract
        logger.info("🔄 Extrahiere Daten...")
        
        # 2. Transform  
        logger.info("🔄 Transformiere Daten...")
        
        # 3. Load
        logger.info("🔄 Lade Daten...")
        
        logger.info("ETL-Pipeline erfolgreich abgeschlossen")
        
    except Exception as e:
        logger.error(f"ETL-Pipeline Fehler: {{e}}")
        raise

if __name__ == "__main__":
    etl_pipeline()
'''
            return (code, f"⚠️ Fallback-Template: {str(e)}")


def launch():
    """Startet Gradio Interface"""
    interface_manager = ETLGradioInterface()
    interface = interface_manager.create_interface()
    interface.launch(server_name="0.0.0.0", server_port=7860, share=False, debug=True)


# Füge diese Zeile hinzu für python -m Ausführung
if __name__ == "__main__":
    launch()
