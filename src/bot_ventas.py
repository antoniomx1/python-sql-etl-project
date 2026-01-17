import os
import logging
import psycopg2
from psycopg2 import extras
import requests
from dotenv import load_dotenv
from datetime import datetime

# =============================================================================
# CONFIGURACIÓN DE LOGGING
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

load_dotenv()

class SalesBot:
    """
    Clase encargada de la extracción y envío de métricas de colocación
    de préstamos para distribuidores hacia Telegram.
    """

    def __init__(self):
        self.token = os.getenv("TELEGRAM_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.db_config = {
            "host": os.getenv("DB_HOST"),
            "port": os.getenv("DB_PORT"),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASS")
        }
        # Fecha de corte según el caso práctico
        self.fecha_corte = "2025-06-14"

    def get_sales_data(self):
        query_metrics = """
            SELECT 
                SUM(CASE WHEN fecha_trx::date = %s THEN monto ELSE 0 END) as diaria,
                SUM(monto) as acumulado_mes
            FROM fct_transacciones
            WHERE fecha_trx >= '2025-06-01' AND fecha_trx <= %s;
        """
        
        # Con LEFT JOIN y COALESCE para que nada se nos escape
        query_distribuidores = """
            SELECT 
                COALESCE(d.nombre_distribuidor, 'Venta Directa') as nombre_distribuidor, 
                SUM(f.monto) as total_prestamos
            FROM fct_transacciones f
            LEFT JOIN dim_clientes c ON f.id_cliente = c.id_cliente
            LEFT JOIN dim_distribuidores d ON c.id_distribuidor = d.id_distribuidor
            WHERE f.fecha_trx::date = %s
            GROUP BY 1
            ORDER BY total_prestamos DESC;
        """
        
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=extras.RealDictCursor) as cur:
                    cur.execute(query_metrics, (self.fecha_corte, self.fecha_corte))
                    metrics = cur.fetchone()
                    
                    cur.execute(query_distribuidores, (self.fecha_corte,))
                    distribuidores = cur.fetchall()
                    
                    return metrics, distribuidores
        except Exception as e:
            logging.error(f"Error en la base de datos: {e}")
            return None, None

    def format_message(self, metrics, distribuidores):
        """
        Genera el reporte con terminología de colocación y préstamos.
        """
        meses_es = {
            1: "ENE", 2: "FEB", 3: "MAR", 4: "ABR", 
            5: "MAY", 6: "JUN", 7: "JUL", 8: "AGO", 
            9: "SEP", 10: "OCT", 11: "NOV", 12: "DIC"
        }

        fecha_dt = datetime.strptime(self.fecha_corte, "%Y-%m-%d")
        mes_texto = meses_es[fecha_dt.month]
        fecha_formateada = f"{fecha_dt.day} {mes_texto}, {fecha_dt.year}"

        url_looker = "https://lookerstudio.google.com/reporting/1b952e87-75af-4570-b81f-a7d0191a095b"

        reporte = (
            f"REPORTE DE COLOCACIÓN - PRÉSTAMOS\n"
            f"FECHA DE CORTE: {fecha_formateada}\n"
            f"{'=' * 30}\n\n"
            f"PRÉSTAMOS DEL DÍA: ${metrics['diaria']:,.2f}\n"
            f"ACUMULADO MENSUAL: ${metrics['acumulado_mes']:,.2f}\n\n"
            f"RENDIMIENTO POR DISTRIBUIDORA:\n"
        )
        
        for dist in distribuidores:
            reporte += f"- {dist['nombre_distribuidor']}: ${dist['total_prestamos']:,.2f}\n"
            
        reporte += (
            f"\nANÁLISIS DETALLADO:\n"
            f"[CONSULTAR DASHBOARD COMPLETO]({url_looker})\n"
        )
        
        return reporte

    def send_to_telegram(self, text):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "Markdown"
        }
        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            logging.info("Reporte enviado exitosamente.")
        except Exception as e:
            logging.error(f"Error enviando a Telegram: {e}")

    def run(self):
        logging.info("Iniciando generación de Rep de Colocación...")
        metrics, distribuidores = self.get_sales_data()
        
        if metrics and distribuidores:
            mensaje = self.format_message(metrics, distribuidores)
            self.send_to_telegram(mensaje)
        else:
            logging.error("No se pudieron obtener datos para el reporte.")

if __name__ == "__main__":
    bot = SalesBot()
    bot.run()