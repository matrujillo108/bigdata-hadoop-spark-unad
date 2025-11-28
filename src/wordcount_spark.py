"""
WORDCOUNT_SPARK.PY - Contador de Palabras con Apache Spark
==========================================================
Estudiante: Mario Trujillo
Cédula: 1192778801
Curso: Big Data Integration - UNAD
Código: 203008077
Fecha: Noviembre 2024

Descripción: Implementación de WordCount usando PySpark RDD
para procesar el archivo Application.txt desde HDFS.
"""

from pyspark import SparkContext, SparkConf
from datetime import datetime
import time
import os

def main():
    # Configuración de Spark
    conf = SparkConf().setAppName("WordCountUNAD").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    
    print("=" * 70)
    print("PYSPARK WORDCOUNT - BIG DATA INTEGRATION")
    print("ESTUDIANTE: MARIO TRUJILLO - CÉDULA: 1192778801")
    print("=" * 70)
    
    try:
        start_time = time.time()
        
        # 1. CARGAR ARCHIVO DESDE HDFS
        print("📁 Cargando archivo desde HDFS...")
        text_file = sc.textFile("hdfs://localhost:9000/input/Application.txt")
        print("✅ Archivo cargado exitosamente")
        
        # 2. CONTAR TOTAL DE LÍNEAS (para diagnóstico)
        line_count = text_file.count()
        print(f"📄 Total de líneas en el archivo: {line_count}")
        
        # 3. PROCESAMIENTO CON RDD - WORDCOUNT
        print("🔄 Procesando palabras con Spark RDD...")
        
        # Transformaciones RDD:
        # flatMap → Divide líneas en palabras
        # map → Convierte cada palabra en (palabra, 1)
        # reduceByKey → Suma los valores para cada palabra
        counts_rdd = (text_file
            .flatMap(lambda line: line.split())
            .map(lambda word: (word.strip(), 1))
            .reduceByKey(lambda a, b: a + b))
        
        # 4. OBTENER RESULTADOS
        print("📊 Calculando resultados...")
        all_results = counts_rdd.collect()
        
        # 5. BUSCAR PALABRAS ESPECÍFICAS SOLICITADAS
        target_words = ["Recognition", "Vision", "Robotics", "Assistants", "Predictive"]
        results_dict = {}
        
        for word, count in all_results:
            if word in target_words:
                results_dict[word] = count
        
        # Asegurar que todas las palabras objetivo tengan valor
        for word in target_words:
            if word not in results_dict:
                results_dict[word] = 0
        
        # 6. MOSTRAR RESULTADOS EN CONSOLA
        print("\n" + "🎯" * 20)
        print("RESULTADOS DEL CONTEO")
        print("🎯" * 20)
        
        print(f"\n📈 PALABRAS SOLICITADAS:")
        for word in target_words:
            print(f"   🔹 {word:<12}: {results_dict[word]:>3} apariciones")
        
        print(f"\n📊 ESTADÍSTICAS GENERALES:")
        print(f"   Total palabras únicas: {len(all_results)}")
        
        # Calcular total de ocurrencias
        total_occurrences = sum(count for _, count in all_results)
        print(f"   Total de ocurrencias: {total_occurrences}")
        
        # 7. GUARDAR RESULTADOS COMPLETOS EN HDFS
        print("\n💾 Guardando resultados en HDFS...")
        
        # Eliminar directorio anterior si existe
        print("🗑️  Eliminando directorio de salida anterior...")
        os.system("hdfs dfs -rm -r /output_spark_final 2>/dev/null")
        
        counts_rdd.saveAsTextFile("hdfs://localhost:9000/output_spark_final")
        print("✅ Resultados guardados en: /output_spark_final")
        
        # 8. GENERAR REPORTE HTML PROFESIONAL
        print("📄 Generando reporte HTML...")
        generate_html_report(results_dict, all_results, execution_time=time.time() - start_time)
        
        # 9. MOSTRAR TIEMPO DE EJECUCIÓN
        execution_time = time.time() - start_time
        print(f"\n⏱️  TIEMPO DE EJECUCIÓN: {execution_time:.2f} segundos")
        print("✅ PROCESO COMPLETADO EXITOSAMENTE!")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # 10. CERRAR CONTEXTO DE SPARK
        sc.stop()
        print("🔚 Contexto de Spark cerrado")

def generate_html_report(results_dict, all_results, execution_time):
    """Genera un reporte HTML profesional con los resultados"""
    
    html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Resultados WordCount - Spark</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            color: #333;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 40px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #2c3e50 0%, #3498db 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        .student-info {{
            background: #ecf0f1;
            padding: 20px;
            border-bottom: 3px solid #3498db;
        }}
        .results-section {{
            padding: 30px;
        }}
        .target-results {{
            background: #e8f6f3;
            padding: 25px;
            border-radius: 10px;
            margin-bottom: 30px;
            border-left: 5px solid #1abc9c;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }}
        th, td {{
            padding: 15px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #3498db;
            color: white;
            font-weight: 600;
        }}
        tr:hover {{
            background: #f5f5f5;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        .stat-card {{
            background: #3498db;
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }}
        .footer {{
            background: #2c3e50;
            color: white;
            text-align: center;
            padding: 20px;
            margin-top: 30px;
        }}
        .success-badge {{
            background: #2ecc71;
            color: white;
            padding: 10px 20px;
            border-radius: 20px;
            display: inline-block;
            margin: 10px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 WordCount con Apache Spark</h1>
            <p>Análisis de Datos con PySpark RDD</p>
        </div>
        
        <div class="student-info">
            <h2>👨‍🎓 Información del Estudiante</h2>
            <p><strong>Nombre:</strong> Mario Trujillo</p>
            <p><strong>Cédula:</strong> 1192778801</p>
            <p><strong>Curso:</strong> Big Data Integration - UNAD</p>
            <p><strong>Código:</strong> 203008077</p>
            <p><strong>Fecha de generación:</strong> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</p>
        </div>
        
        <div class="results-section">
            <div class="success-badge">
                ✅ PROCESAMIENTO EXITOSO CON SPARK RDD
            </div>
            
            <div class="target-results">
                <h2>🎯 Resultados de Palabras Solicitadas</h2>
                <table>
                    <tr>
                        <th>Palabra</th>
                        <th>Conteo</th>
                        <th>Estado</th>
                    </tr>"""
    
    # Agregar filas para palabras específicas
    for word in ["Recognition", "Vision", "Robotics", "Assistants", "Predictive"]:
        count = results_dict.get(word, 0)
        status = "✅ Encontrada" if count > 0 else "⚠️ No encontrada"
        html_content += f"""
                    <tr>
                        <td><strong>{word}</strong></td>
                        <td>{count}</td>
                        <td>{status}</td>
                    </tr>"""
    
    html_content += f"""
                </table>
            </div>
            
            <div class="stats">
                <div class="stat-card">
                    <h3>📊 Total Palabras Únicas</h3>
                    <p style="font-size: 2em; margin: 10px 0;">{len(all_results)}</p>
                </div>
                <div class="stat-card">
                    <h3>⏱️ Tiempo Ejecución</h3>
                    <p style="font-size: 2em; margin: 10px 0;">{execution_time:.2f}s</p>
                </div>
                <div class="stat-card">
                    <h3>🔍 Tecnología</h3>
                    <p style="font-size: 1.2em; margin: 10px 0;">PySpark RDD</p>
                </div>
            </div>
            
            <h2>🛠️ Método Utilizado</h2>
            <ul>
                <li><strong>Almacenamiento:</strong> Hadoop HDFS</li>
                <li><strong>Procesamiento:</strong> Apache Spark 3.x</li>
                <li><strong>Lenguaje:</strong> Python con PySpark</li>
                <li><strong>Estructura:</strong> RDD (Resilient Distributed Dataset)</li>
                <li><strong>Transformaciones:</strong> flatMap → map → reduceByKey</li>
            </ul>
        </div>
        
        <div class="footer">
            <p>Universidad Nacional Abierta y a Distancia - UNAD</p>
            <p>Big Data Integration - 2024</p>
        </div>
    </div>
</body>
</html>"""
    
    # Guardar archivo HTML
    html_filename = "/home/hadoop/spark_results_final.html"
    with open(html_filename, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"✅ Reporte HTML generado: {html_filename}")

if __name__ == "__main__":
    main()
