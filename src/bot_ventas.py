import os
import logging
import psycopg2
from psycopg2 import extras
import requests
from dotenv import load_dotenv
from datetime import datetime

# =============================================================================
# CONFIGURACIÓN DE LOGGING
# Se establece un formato detallado para seguimiento de eventos y errores.
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Carga de variables de entorno desde el archivo .env
load_dotenv()

class SalesBot:
    """
    Clase encargada de la extracción, procesamiento y envío de métricas 
    comerciales de Tienda Pago hacia Telegram.
    """

    def __init__(self):
        # Inicialización de credenciales y parámetros de conexión
        self.token = os.getenv("TELEGRAM_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASS")
        }
        # Definición de la fecha de corte para el reporte histórico (Simulación)
        self.fecha_corte = "2025-06-14"

    def get_sales_data(self):
        """
        Ejecuta consultas SQL para obtener la venta diaria, el acumulado 
        mensual y el rendimiento por sede geográfica.
        """
        # Query 1: Obtención de métricas agregadas de tiempo
        query_metrics = """
            SELECT 
                SUM(CASE WHEN fecha_trx = %s THEN monto ELSE 0 END) as diaria,
                SUM(monto) as acumulado_mes
            FROM fct_transacciones
            WHERE fecha_trx >= '2025-06-01' AND fecha_trx <= %s;
        """
        # Query 2: Desglose de ingresos por sede
        query_sedes = """
            SELECT s.nombre_sede, SUM(f.monto) as total_sede
            FROM fct_transacciones f
            JOIN dim_sedes s ON f.id_sede = s.id_sede
            WHERE f.fecha_trx = %s
            GROUP BY s.nombre_sede
            ORDER BY total_sede DESC;
        """
        
        try:
            # Establecimiento de conexión con PostgreSQL
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                    # Extracción de métricas de desempeño global
                    cur.execute(query_metrics, (self.fecha_corte, self.fecha_corte))
                    metrics = cur.fetchone()
                    
                    # Extracción de métricas detalladas por zona
                    cur.execute(query_sedes, (self.fecha_corte,))
                    sedes = cur.fetchall()
                    
                    return metrics, sedes
        except Exception as e:
            logging.error(f"Falla en la conexión o ejecución SQL: {e}")
            return None, None

    def format_message(self, metrics, sedes):
        """
        Construye el cuerpo del mensaje con formato profesional y 
        enlace directo al dashboard de Looker Studio.
        """
        # Mapeo de meses para asegurar consistencia en español
        meses_es = {
            1: "ENE", 2: "FEB", 3: "MAR", 4: "ABR", 
            5: "MAY", 6: "JUN", 7: "JUL", 8: "AGO", 
            9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC"
        }

        # Procesamiento de la fecha
        fecha_dt = datetime.strptime(self.fecha_corte, "%Y-%m-%d")
        mes_texto = meses_es[fecha_dt.month]
        fecha_formateada = f"{fecha_dt.day} {mes_texto}, {fecha_dt.year}"

        # URL de tu Dashboard de Looker Studio (Pega aquí tu link real)
        url_looker = "https://lookerstudio.google.com/reporting/1b952e87-75af-4570-b81f-a7d0191a095b"

        # Construcción del cuerpo del reporte
        reporte = (
            f"REPORTE COMERCIAL\n"
            f"FECHA DE CORTE: {fecha_formateada}\n"
            f"{'=' * 30}\n\n"
            f"VENTA DEL DÍA: ${metrics['diaria']:,.2f}\n"
            f"ACUMULADO MENSUAL: ${metrics['acumulado_mes']:,.2f}\n\n"
            f"DESGLOSE POR ZONA (DIARIO):\n"
        )
        
        for sede in sedes:
            reporte += f"- {sede['nombre_sede']}: ${sede['total_sede']:,.2f}\n"
            
        # Sección de acceso a detalles detallados
        reporte += (
            f"\nANÁLISIS DETALLADO:\n"
            f"[CONSULTAR DASHBOARD COMPLETO]({url_looker})\n"
        )
            
        
        return reporte

    def send_to_telegram(self, text):
        """
        Realiza la petición POST a la API de Telegram para la entrega del mensaje.
        """
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown"
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            logging.info("Entrega de reporte confirmada.")
        except Exception as e:
            logging.error(f"Error en la comunicación con Telegram API: {e}")

    def run(self):
        """Orquestador principal del proceso."""
        logging.info("Iniciando generación de Flash de Ventas...")
        metrics, sedes = self.get_sales_data()
        
        if metrics and sedes:
            mensaje = self.format_message(metrics, sedes)
            self.send_to_telegram(mensaje)
        else:
            logging.error("Proceso interrumpido: No se obtuvieron datos de la fuente.")

if __name__ == "__main__":
    bot = SalesBot()
    bot.run()
