from typing import Any, Dict, List, Union
import sqlite3
import os
import sys
from mcp.server.fastmcp import FastMCP



SQLITE_DB_PATH = "/workspaces/MCP_Projects/gold_price.db"

# Initialize MCP server
mcp = FastMCP("gold_price_query")

def check_database_exists() -> bool:
    """Check if the database file exists and has the required table."""
    if not os.path.exists(SQLITE_DB_PATH):
        return False
    
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='gold_price'")
        table_exists = cursor.fetchone() is not None
        conn.close()
        return table_exists
    except sqlite3.Error:
        return False

def is_safe_sql(sql: str) -> bool:
    """Check if the SQL query is a safe SELECT on gold_price."""
    if not sql or not isinstance(sql, str):
        return False
    
    sql_lower = sql.strip().lower()
    
    # Must start with SELECT
    if not sql_lower.startswith("select"):
        return False
    
    # Must reference gold_price table
    if "gold_price" not in sql_lower:
        return False
    
    # Check for forbidden keywords
    forbidden_keywords = [
        "insert", "update", "delete", "drop", "alter", "create", 
        "truncate", "replace", "pragma", "attach", "detach"
    ]
    
    # Check for semicolon followed by more content (SQL injection)
    if ";" in sql_lower and sql_lower.rstrip().rstrip(';') != sql_lower.rstrip():
        return False
    
    # Check for forbidden keywords
    for keyword in forbidden_keywords:
        if keyword in sql_lower:
            return False
    
    return True

def execute_sql_query(sql: str) -> Union[Dict[str, Any], Dict[str, str]]:
    """Safely execute a SQL query on the SQLite database."""
    
    # Check if database exists
    if not check_database_exists():
        return {
            "error": f"Database not found or invalid. Please run 'python create_sample_db.py' first.",
            "database_path": os.path.abspath(SQLITE_DB_PATH)
        }
    
    # Validate SQL safety
    if not is_safe_sql(sql):
        return {"error": "Only safe SELECT queries on the gold_price table are allowed."}

    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row  # return dict-like rows

        with conn:
            cursor = conn.execute(sql)
            rows = cursor.fetchall()
            result = [dict(row) for row in rows]
            
            # Return structured response
            return {
                "success": True,
                "data": result,
                "row_count": len(result),
                "query": sql
            }

    except sqlite3.Error as e:
        return {"error": f"SQLite error: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
    finally:
        if 'conn' in locals():
            conn.close()

@mcp.tool()
def run_query(sql: str) -> Dict[str, Any]:
    """
    Run a SELECT SQL query on the gold_price table.

    Args:
        sql: A SELECT query referencing the 'gold_price' table

    Returns:
        Dictionary containing query results or error message
    """
    return execute_sql_query(sql)

@mcp.tool()
def get_table_info() -> Dict[str, Any]:
    """
    Get information about the gold_price table structure.
    
    Returns:
        Dictionary containing table schema information
    """
    if not check_database_exists():
        return {
            "error": f"Database not found or invalid. Please run 'python create_sample_db.py' first.",
            "database_path": os.path.abspath(SQLITE_DB_PATH)
        }
    
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        
        with conn:
            # Get table schema
            cursor = conn.execute("PRAGMA table_info(gold_price)")
            columns = [dict(row) for row in cursor.fetchall()]
            
            # Get row count
            cursor = conn.execute("SELECT COUNT(*) as count FROM gold_price")
            row_count = cursor.fetchone()['count']
            
            return {
                "success": True,
                "table_name": "gold_price",
                "columns": columns,
                "row_count": row_count,
                "database_path": os.path.abspath(SQLITE_DB_PATH)
            }
    
    except sqlite3.Error as e:
        return {"error": f"SQLite error: {str(e)}"}
    except Exception as e:
        return {"error": f"Unexpected error: {str(e)}"}
    finally:
        if 'conn' in locals():
            conn.close()

@mcp.tool()
def sample_data() -> Dict[str, Any]:
    """
    Get a sample of data from the gold_price table.
    
    Returns:
        Dictionary containing sample data or error message
    """
    return execute_sql_query("SELECT * FROM gold_price ORDER BY date DESC LIMIT 5")

@mcp.tool()
def get_latest_price() -> Dict[str, Any]:
    """
    Get the most recent gold price entry.
    
    Returns:
        Dictionary containing the latest price data or error message
    """
    return execute_sql_query("SELECT * FROM gold_price ORDER BY date DESC LIMIT 1")

@mcp.tool()
def get_price_range(days: int = 7) -> Dict[str, Any]:
    """
    Get gold prices for the last N days.
    
    Args:
        days: Number of days to retrieve (default: 7)
    
    Returns:
        Dictionary containing price data or error message
    """
    if days < 1 or days > 365:
        return {"error": "Days must be between 1 and 365"}
    
    return execute_sql_query(f"SELECT * FROM gold_price ORDER BY date DESC LIMIT {days}")

@mcp.tool()
def database_status() -> Dict[str, Any]:
    """
    Check the status of the database and provide setup instructions if needed.
    
    Returns:
        Dictionary containing database status information
    """
    db_path = os.path.abspath(SQLITE_DB_PATH)
    
    if not os.path.exists(SQLITE_DB_PATH):
        return {
            "status": "missing",
            "message": "Database file does not exist",
            "database_path": db_path,
            "setup_instructions": "Run 'python create_sample_db.py' to create the database with sample data"
        }
    
    if not check_database_exists():
        return {
            "status": "invalid",
            "message": "Database exists but gold_price table is missing",
            "database_path": db_path,
            "setup_instructions": "Run 'python create_sample_db.py' to recreate the database"
        }
    
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.execute("SELECT COUNT(*) as count FROM gold_price")
        row_count = cursor.fetchone()[0]
        conn.close()
        
        return {
            "status": "ready",
            "message": f"Database is ready with {row_count} records",
            "database_path": db_path,
            "record_count": row_count
        }
    except sqlite3.Error as e:
        return {
            "status": "error",
            "message": f"Database error: {str(e)}",
            "database_path": db_path
        }

if __name__ == "__main__":
    print(f"🚀 Starting MCP server for gold price data...")
    print(f"📁 Database path: {os.path.abspath(SQLITE_DB_PATH)}")
    print(f"🏷️ Server name: gold_price_query")
    
    # Check database status
    if not check_database_exists():
        print(f"⚠️ Warning: Database file {SQLITE_DB_PATH} not found or invalid!")
        print(f"📝 Please run 'python create_sample_db.py' first to create the database.")
        print(f"🔧 The server will start but tools may return errors until database is created.")
    else:
        print(f"✅ Database ready!")
    
    print(f"🛠️ Available tools: run_query, get_table_info, sample_data, get_latest_price, get_price_range, database_status")
    
    try:
        mcp.run(transport="sse")
    except KeyboardInterrupt:
        print("\n👋 Server stopped by user")
    except Exception as e:
        print(f"❌ Server error: {e}", file=sys.stderr)
        sys.exit(1)