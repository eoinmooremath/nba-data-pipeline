import warnings
warnings.filterwarnings('ignore', category=UserWarning)
import sys
import time
from datetime import datetime
from utils.db_utils import get_db_connection


def log_message(message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - {message}")
    with open('/var/log/nba_backup.log', 'a') as f:
        f.write(f"{timestamp} - {message}\n")
        f.flush()


def write_to_file(filename, content):
    with open(filename, 'a', encoding='utf-8') as f:
        f.write(content + '\n')

def format_value(value):
    if value is None:
        return 'NULL'
    elif isinstance(value, (int, float)):
        return str(value)
    elif isinstance(value, bool):
        return '1' if value else '0'
    else:
        return "'" + str(value).replace("'", "''") + "'"

def main():
    try:
        log_message("Starting SQL dump creation")
        conn = get_db_connection()
        cursor = conn.cursor()

        # Start the SQL file
        with open('NBA_Database.sql', 'w', encoding='utf-8') as f:
            f.write('USE master;\nGO\n\n')
            f.write('IF DB_ID(\'NBA_Database\') IS NOT NULL\n\tDROP DATABASE NBA_Database;\nGO\n\n')
            f.write('CREATE DATABASE NBA_Database;\nGO\n\n')
            f.write('USE NBA_Database;\nGO\n\n')

        # Get and create tables
        cursor.execute("""
        SELECT
            OBJECT_SCHEMA_NAME(o.object_id) as schema_name,
            o.name as table_name,
            c.name as column_name,
            t.name as data_type,
            c.max_length,
            c.precision,
            c.scale,
            c.is_nullable,
            c.is_identity
        FROM sys.objects o
        JOIN sys.columns c ON o.object_id = c.object_id
        JOIN sys.types t ON c.user_type_id = t.user_type_id
        WHERE o.type = 'U'
        ORDER BY o.name, c.column_id
        """)

        current_table = None
        columns = []

        for row in cursor.fetchall():
            if current_table != row.table_name:
                if columns:
                    write_to_file('NBA_Database.sql', ','.join(columns) + '\n);\nGO\n\n')
                current_table = row.table_name
                columns = [f"CREATE TABLE [{row.schema_name}].[{row.table_name}] ("]

            column_def = f"\n    [{row.column_name}] [{row.data_type}]"

            if row.data_type in ('varchar', 'nvarchar', 'char', 'nchar'):
                column_def += f"({row.max_length if row.max_length != -1 else 'MAX'})"
            elif row.data_type in ('decimal', 'numeric'):
                column_def += f"({row.precision},{row.scale})"

            column_def += ' NULL' if row.is_nullable else ' NOT NULL'

            if row.is_identity:
                column_def += ' IDENTITY(1,1)'

            columns.append(column_def)

        if columns:
            write_to_file('NBA_Database.sql', ','.join(columns) + '\n);\nGO\n\n')

        # Export data
        cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = cursor.fetchall()

        for table in tables:
            log_message(f"Exporting data for {table.TABLE_NAME}")
            write_to_file('NBA_Database.sql', f'SET IDENTITY_INSERT [{table.TABLE_SCHEMA}].[{table.TABLE_NAME}] ON;\nGO\n')

            offset = 0
            chunk_size = int(os.getenv('EXPORT_CHUNK_SIZE', 50000))

            while True:
                cursor.execute(f"""
                SELECT *
                FROM [{table.TABLE_SCHEMA}].[{table.TABLE_NAME}]
                ORDER BY (SELECT NULL)
                OFFSET {offset} ROWS
                FETCH NEXT {chunk_size} ROWS ONLY
                """)

                rows = cursor.fetchall()
                if not rows:
                    break

                for row in rows:
                    values = [format_value(val) for val in row]
                    write_to_file('NBA_Database.sql',
                                f"INSERT INTO [{table.TABLE_SCHEMA}].[{table.TABLE_NAME}] VALUES ({', '.join(values)});\n")

                offset += chunk_size
                log_message(f"Processed {offset} rows for {table.TABLE_NAME}")

            write_to_file('NBA_Database.sql', f'\nSET IDENTITY_INSERT [{table.TABLE_SCHEMA}].[{table.TABLE_NAME}] OFF;\nGO\n\n')

        log_message("SQL dump creation completed successfully")

    except Exception as e:
        log_message(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
