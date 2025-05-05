import happybase
import pandas as pd

def main():
    try:
        # 1. Conexión con HBase
        print("Conectando a HBase...")
        connection = happybase.Connection('localhost')
        print("Conexión exitosa con HBase")

        # 2. Configuración de la tabla
        table_name = 'ghibli_image_analysis'
        
        # Familias de columnas
        families = {
            'basic_data': dict(),       # Datos básicos
            'performance': dict(),       # Métricas de rendimiento
            'engagement': dict(),        # Interacción social
            'technical': dict()          # Datos técnicos
        }

        # Eliminar tabla si existe y crear nueva
        if table_name.encode() in connection.tables():
            print("Eliminando tabla existente...")
            connection.delete_table(table_name, disable=True)
        
        print("Creando nueva tabla...")
        connection.create_table(table_name, families)
        table = connection.table(table_name)
        print("Tabla creada exitosamente")

        # 3. Cargar datos desde CSV
        print("\nCargando datos del CSV...")
        df = pd.read_csv('ai_ghibli_trend_dataset.csv')
        
        # Insertar datos en HBase
        for index, row in df.iterrows():
            row_key = f"img_{index}".encode()
            
            data = {
                b'basic_data:prompt': str(row['prompt']).encode(),
                b'basic_data:platform': str(row['platform']).encode(),
                b'basic_data:creation_date': str(row['creation_date']).encode(),
                
                b'performance:generation_time': str(row['generation_time']).encode(),
                b'performance:gpu_usage': str(row['gpu_usage']).encode(),
                b'performance:file_size': str(row['file_size_kb']).encode(),
                
                b'engagement:likes': str(row['likes']).encode(),
                b'engagement:shares': str(row['shares']).encode(),
                b'engagement:comments': str(row['comments']).encode(),
                b'engagement:top_comment': str(row['top_comment']).encode(),
                
                b'technical:resolution': str(row['resolution']).encode(),
                b'technical:style_score': str(row['style_accuracy_score']).encode(),
                b'technical:is_hand_edited': str(row['is_hand_edited']).encode(),
                b'technical:ethical_flag': str(row['ethical_concerns_flag']).encode()
            }
            
            table.put(row_key, data)
        
        print(f"Datos cargados: {len(df)} registros")

        # 4. Análisis de datos

        def calculate_average(column_family, column):
            total = 0
            count = 0
            col = f"{column_family}:{column}".encode()
            for _, data in table.scan(columns=[col]):
                try:
                    total += float(data[col].decode())
                    count += 1
                except:
                    continue
            return total / count if count > 0 else 0

        def get_top_10(column_family, metric_column, name_column='prompt'):
            results = []
            col_metric = f"{column_family}:{metric_column}".encode()
            col_name = f"basic_data:{name_column}".encode()
            
            for key, data in table.scan(columns=[col_name, col_metric]):
                results.append({
                    'name': data[col_name].decode(),
                    'value': float(data[col_metric].decode())
                })
            
            return sorted(results, key=lambda x: x['value'], reverse=True)[:10]

        # a. Top 10 imágenes que más GPU consumieron
        print("\n=== Top 10 imágenes por consumo de GPU ===")
        for item in get_top_10('performance', 'gpu_usage'):
            print(f"{item['name'][:50]}...: {item['value']}")

        print(f"\nPromedio de GPU: {calculate_average('performance', 'gpu_usage'):.2f}")

        # b. Top 10 imágenes por tamaño de archivo
        print("\n=== Top 10 imágenes por tamaño (KB) ===")
        for item in get_top_10('performance', 'file_size'):
            print(f"{item['name'][:50]}...: {item['value']} KB")

        print(f"\nPromedio de tamaño: {calculate_average('performance', 'file_size'):.2f} KB")

        # c. Top 10 imágenes por tiempo de generación
        print("\n=== Top 10 imágenes por tiempo de generación ===")
        for item in get_top_10('performance', 'generation_time'):
            print(f"{item['name'][:50]}...: {item['value']} segundos")

        print(f"\nTiempo promedio: {calculate_average('performance', 'generation_time'):.2f} seg")

        # d. Top 10 imágenes más compartidas
        print("\n=== Top 10 imágenes más compartidas ===")
        for item in get_top_10('engagement', 'shares'):
            print(f"{item['name'][:50]}...: {int(item['value'])} shares")

        print(f"\nPromedio de shares: {calculate_average('engagement', 'shares'):.2f}")

        # e. Top 10 imágenes con más likes
        print("\n=== Top 10 imágenes con más likes ===")
        for item in get_top_10('engagement', 'likes'):
            print(f"{item['name'][:50]}...: {int(item['value'])} likes")

        print(f"\nPromedio de likes: {calculate_average('engagement', 'likes'):.2f}")

        # f. Top 10 imágenes con más comentarios
        print("\n=== Top 10 imágenes con más comentarios ===")
        for item in get_top_10('engagement', 'comments'):
            print(f"{item['name'][:50]}...: {int(item['value'])} comentarios")

        print(f"\nPromedio de comentarios: {calculate_average('engagement', 'comments'):.2f}")

        # g. Plataformas más usadas para trend Ghibli
        print("\n=== Plataformas más populares ===")
        platforms = {}
        for _, data in table.scan(columns=[b'basic_data:platform']):
            platform = data[b'basic_data:platform'].decode()
            platforms[platform] = platforms.get(platform, 0) + 1
        
        for platform, count in sorted(platforms.items(), key=lambda x: x[1], reverse=True):
            print(f"{platform}: {count} imágenes")

    except Exception as e:
        print(f"\nError: {str(e)}")
    finally:
        if 'connection' in locals():
            connection.close()
            print("\nConexión cerrada")

if __name__ == "__main__":
    main()
